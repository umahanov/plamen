package log

import (
	"errors"
	"fmt"
	api "github.com/umahanov/plamen/api/v1"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"path"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{baseOffset: baseOffset, config: c}
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644,
	)
	if err != nil {
		return nil, err
	}

	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}
	idxFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	s.index, err = newIndex(idxFile, c)
	if err != nil {
		return nil, err
	}

	// obtain the last relative offset of this segment
	if off, _, err := s.index.Read(-1); errors.Is(err, io.EOF) {
		// if segment is empty
		s.nextOffset = s.baseOffset
	} else {
		// absoluteLastOffset = lastSegmentOffset + baseOffset
		// nextOffset = absoluteLastOffset + 1
		s.nextOffset = s.baseOffset + uint64(off) + 1
	}
	return s, nil
}

func (s *segment) Append(r *api.Record) (offset uint64, err error) {
	insertedOff := s.nextOffset
	r.Offset = insertedOff
	p, err := proto.Marshal(r)
	if err != nil {
		return 0, err
	}
	_, startByteNum, err := s.store.append(p)
	if err != nil {
		return 0, err
	}
	if err := s.index.Write(uint32(s.nextOffset-s.baseOffset), startByteNum); err != nil {
		return 0, err
	}
	s.nextOffset++
	return insertedOff, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	off = off - s.baseOffset
	_, pos, err := s.index.Read(int64(off))
	if err != nil {
		return nil, err
	}
	p, err := s.store.read(pos)
	if err != nil {
		return nil, err
	}
	r := &api.Record{}
	proto.Unmarshal(p, r)
	return r, err
}

func (s *segment) isMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
