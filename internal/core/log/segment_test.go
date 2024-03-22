package log

import (
	"github.com/stretchr/testify/require"
	api "github.com/umahanov/plamen/api/v1"
	"io"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("record")}

	c := Config{}
	c.Segment.MaxIndexBytes = 3 * iRecSize
	c.Segment.MaxStoreBytes = 1024

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.baseOffset, s.nextOffset)
	require.False(t, s.isMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		r, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, r.Value, want.Value)
	}

	_, err = s.Append(want)
	require.ErrorIs(t, err, io.EOF)
	require.True(t, s.isMaxed())

	c.Segment.MaxIndexBytes = 1024
	c.Segment.MaxStoreBytes = 3 * uint64(len(want.Value))

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, s.isMaxed())

	err = s.Remove()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.isMaxed())
}
