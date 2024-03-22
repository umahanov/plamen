package log

import (
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp("", "tempfile")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.file.Name())

	entities := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 1},
		{Off: 1, Pos: 10},
	}

	for _, r := range entities {
		err := idx.Write(r.Off, r.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(r.Off))
		require.NoError(t, err)
		require.Equal(t, pos, r.Pos)
	}

	_, _, err = idx.Read(int64(len(entities)))
	require.Error(t, err)
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0644)
	idx, err = newIndex(f, c)
	require.NoError(t, err)

	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entities[1].Pos, pos)
}
