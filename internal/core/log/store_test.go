package log

import (
	"encoding/binary"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var (
	payload         = []byte("record")
	pSizeWithOffset = uint64(len(payload) + offsetSize)
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 10; i++ {
		bWritten, start, err := s.append(payload)
		require.Equal(t, len(payload)+offsetSize, int(bWritten))
		require.NoError(t, err)
		require.Equal(t, bWritten+start, pSizeWithOffset*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var from uint64
	for i := uint64(1); i < 10; i++ {
		r, err := s.read(from)
		require.Equal(t, r, payload)
		require.NoError(t, err)
		from += uint64(pSizeWithOffset)
	}
}

// reading offset to b byte slice, then reading from off
// position b len bytes and comparing that len read equals
// to offset size

// reading from offset (0+8 at 1st iteration) to payload sized
// byte slice and comparing that it equals to predefined payload
// and that read length equals to payload length stored as offset
// value
func testReadAt(t *testing.T, s *store) {
	t.Helper()
	var off int64
	for i := uint64(1); i < 10; i++ {
		b := make([]byte, offsetSize)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, n, offsetSize)
		off += int64(n)

		size := binary.BigEndian.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, b, payload)
		require.Equal(t, n, int(size))
		off += int64(n)
	}
}

func testClose(t *testing.T, s *store) {
	name := "tmpfile"
	f, err := os.CreateTemp("", name)
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err = newStore(f)
	require.NoError(t, err)
	_, _, err = s.append(payload)
	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)
	s.Close()
	f, afterSize, err := openFile(f.Name())
	require.Equal(t, beforeSize+int64(len(payload)), afterSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
