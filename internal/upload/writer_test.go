//go:build unix

package upload

import (
	"bytes"
	"errors"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLimitedWriter_WithinLimit(t *testing.T) {
	var buf bytes.Buffer
	lw := NewLimitedWriter(&buf, 100)

	data := []byte("hello world") // 11 bytes
	n, err := lw.Write(data)

	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, int64(len(data)), lw.Written())
	assert.Equal(t, "hello world", buf.String())
}

func TestLimitedWriter_ExactLimit(t *testing.T) {
	var buf bytes.Buffer
	lw := NewLimitedWriter(&buf, 5)

	n, err := lw.Write([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, int64(5), lw.Written())
}

func TestLimitedWriter_ExceedsLimit(t *testing.T) {
	var buf bytes.Buffer
	lw := NewLimitedWriter(&buf, 10)

	// Первая запись — помещается.
	n, err := lw.Write([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	// Вторая запись — 6 байт, суммарно 11 > 10 — ошибка.
	n, err = lw.Write([]byte("world!"))
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, ErrSizeLimitExceeded)

	// Written не увеличился.
	assert.Equal(t, int64(5), lw.Written())

	// В buf попало только первое.
	assert.Equal(t, "hello", buf.String())
}

func TestLimitedWriter_NoLimit(t *testing.T) {
	var buf bytes.Buffer
	lw := NewLimitedWriter(&buf, 0) // 0 = без лимита

	data := make([]byte, 10000)
	n, err := lw.Write(data)

	require.NoError(t, err)
	assert.Equal(t, 10000, n)
	assert.Equal(t, int64(10000), lw.Written())
}

func TestLimitedWriter_MultipleWrites(t *testing.T) {
	var buf bytes.Buffer
	lw := NewLimitedWriter(&buf, 20)

	for i := 0; i < 4; i++ {
		n, err := lw.Write([]byte("abcde"))
		require.NoError(t, err)
		assert.Equal(t, 5, n)
	}

	// 5-я запись — 25 > 20.
	n, err := lw.Write([]byte("x"))
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, ErrSizeLimitExceeded)
	assert.Equal(t, int64(20), lw.Written())
}

// enospcWriter симулирует ENOSPC при записи.
type enospcWriter struct{}

func (w *enospcWriter) Write([]byte) (int, error) {
	return 0, syscall.ENOSPC
}

func TestLimitedWriter_ENOSPC(t *testing.T) {
	lw := NewLimitedWriter(&enospcWriter{}, 1000)

	n, err := lw.Write([]byte("data"))
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, ErrDiskFull)
}

// failWriter возвращает произвольную ошибку (не ENOSPC).
type failWriter struct{}

func (w *failWriter) Write([]byte) (int, error) {
	return 0, errors.New("disk on fire")
}

func TestLimitedWriter_OtherError(t *testing.T) {
	lw := NewLimitedWriter(&failWriter{}, 1000)

	n, err := lw.Write([]byte("data"))
	assert.Equal(t, 0, n)
	assert.Error(t, err)
	// Не ENOSPC — не должна быть ErrDiskFull.
	assert.NotErrorIs(t, err, ErrDiskFull)
	assert.NotErrorIs(t, err, ErrSizeLimitExceeded)
}

func TestIsENOSPC(t *testing.T) {
	assert.True(t, isENOSPC(syscall.ENOSPC))
	assert.False(t, isENOSPC(errors.New("something else")))
	assert.False(t, isENOSPC(nil))
}
