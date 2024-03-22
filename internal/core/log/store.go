package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

const offsetSize = 8

type store struct {
	l sync.Mutex
	*os.File
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	info, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	return &store{
		l:    sync.Mutex{},
		File: f,
		buf:  bufio.NewWriter(f),
		size: uint64(info.Size()),
	}, nil
}

// append writes the binary offset in BigEndian and non-binary
// record p represented as byte slice to the store buffer
// nn - bytes written
// start - size before append e.g. start point of write (including offset)
func (s *store) append(p []byte) (bytesWritten uint64, start uint64, err error) {
	s.l.Lock()
	defer s.l.Unlock()
	start = s.size
	//write len of appended byte slice to the s.buf as bigendian binary
	if err := binary.Write(s.buf, binary.BigEndian, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	//write payload next to the written len
	nn, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	nn += offsetSize
	s.size += uint64(nn)
	return uint64(nn), start, nil
}

// read reads single record from store buffer starting from start point by
// reading first 8 bytes offset represented as BigEndian,
// then decoding offset to get the length of record payload
// in bytes
func (s *store) read(start uint64) ([]byte, error) {
	s.l.Lock()
	defer s.l.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, offsetSize)
	if _, err := s.File.ReadAt(size, int64(start)); err != nil {
		return nil, err
	}
	rec := make([]byte, binary.BigEndian.Uint64(size))
	if _, err := s.File.ReadAt(rec, int64(start+offsetSize)); err != nil {
		return nil, err
	}
	return rec, nil
}

// readAt reads from the store file, not buffer from the
// off as start point up to p byte slice length bytes
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.l.Lock()
	defer s.l.Unlock()
	if err := s.buf.Flush(); err != nil {

		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.l.Lock()
	defer s.l.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
