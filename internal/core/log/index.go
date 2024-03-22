package log

import (
	"encoding/binary"
	"github.com/tysonmote/gommap"
	"io"
	"os"
)

var (
	offIdx   uint32 = 4
	posIdx   uint64 = 8
	iRecSize        = uint64(offIdx) + posIdx
)

type index struct {
	file *os.File
	size uint64
	mmap gommap.MMap
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := os.Truncate(i.file.Name(), int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

func (i *index) Read(recIdx int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if recIdx == -1 {
		out = uint32(i.size/iRecSize - 1)
	} else {
		out = uint32(recIdx)
	}
	pos = uint64(out) * iRecSize
	if i.size < pos+iRecSize {
		return 0, 0, io.EOF
	}
	out = binary.BigEndian.Uint32(i.mmap[pos : pos+uint64(offIdx)])
	pos = binary.BigEndian.Uint64(i.mmap[pos+uint64(offIdx) : pos+iRecSize])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+iRecSize {
		return io.EOF
	}
	binary.BigEndian.PutUint32(i.mmap[i.size:i.size+uint64(offIdx)], off)
	binary.BigEndian.PutUint64(i.mmap[i.size+uint64(offIdx):i.size+iRecSize], pos)
	i.size += iRecSize
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
