package log

import (
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"path"
)

type Segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func NewSegment(dir string, baseOffset uint64, c Config) (*Segment, error) {
	s := &Segment{baseOffset: baseOffset, config: c}
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.%s", baseOffset, ".store")),
		os.O_RDWR|os.O_APPEND, 0644,
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

func (s *Segment) Append(r *Record) (offset uint64, err error) {
	cur := s.nextOffset
	r.Offset = cur
	p, err := proto.Marshal(r)
	if err != nil {
		return 0, err
	}
	_, start, err := s.store.append(p)
}
