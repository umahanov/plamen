package log

import (
	api "github.com/umahanov/plamen/api/v1"
	"io"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	l             sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	log := &Log{Config: c, Dir: dir}

	return log, log.setup()
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		ext := path.Ext(file.Name())
		if ext == ".index" {
			continue
		}
		offString := strings.TrimSuffix(file.Name(), ext)
		off, _ := strconv.ParseUint(offString, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	slices.Sort(baseOffsets)
	for i := 0; i < len(baseOffsets); i++ {
		if err := l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
	}

	if l.segments == nil {
		if err := l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func (l *Log) Append(r *api.Record) (uint64, error) {
	l.l.Lock()
	defer l.l.Unlock()
	off, err := l.activeSegment.Append(r)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.isMaxed() {
		err = l.newSegment(off + 1)
	}

	return off, err
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.l.RLock()
	defer l.l.RUnlock()
	var s *segment
	for _, seg := range l.segments {
		if seg.baseOffset <= off && off < seg.nextOffset {
			s = seg
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}

	return s.Read(off)
}

func (l *Log) Close() error {
	l.l.Lock()
	defer l.l.Unlock()
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			seg.Close()
		}
	}
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.l.Lock()
	defer l.l.Unlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) LowestHighest() (uint64, error) {
	l.l.Lock()
	defer l.l.Unlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

func (l *Log) Truncate(lowest uint64) error {
	l.l.Lock()
	defer l.l.Unlock()
	var segments []*segment
	for _, seg := range l.segments {
		if seg.nextOffset <= lowest+1 {
			if err := seg.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, seg)
	}
	l.segments = segments

	return nil
}

func (l *Log) Reader() io.Reader {
	l.l.Lock()
	defer l.l.Unlock()
	readers := make([]io.Reader, len(l.segments))
	for i, seg := range l.segments {
		readers[i] = &originReader{seg.store, 0}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}
