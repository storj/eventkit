package resumablecompressed

import (
	"bytes"
	"compress/zlib"
	"crypto/rand"
	"io"
	"testing"
)

func assert(t testing.TB, val bool) {
	t.Helper()
	if !val {
		t.Fatal("assertion failed")
	}
}

type writeNopCloser struct {
	io.Writer
}

func (w writeNopCloser) Close() error { return nil }

func TestResumable(t *testing.T) {
	t.Parallel()

	var sample1 [655360]byte
	_, err := rand.Read(sample1[:])
	assert(t, err == nil)
	var sample2 [655360]byte
	_, err = rand.Read(sample2[:])
	assert(t, err == nil)

	var out bytes.Buffer

	w1, err := NewWriter(writeNopCloser{Writer: &out}, zlib.DefaultCompression)
	assert(t, err == nil)
	_, err = w1.Write(sample1[:])
	assert(t, err == nil)
	assert(t, w1.Close() == nil)

	w2, err := NewWriter(writeNopCloser{Writer: &out}, zlib.DefaultCompression)
	assert(t, err == nil)
	_, err = w2.Write(sample2[:])
	assert(t, err == nil)
	assert(t, w2.Close() == nil)

	r1 := NewReader(&out)
	assert(t, err == nil)
	data, err := io.ReadAll(r1)
	assert(t, err == nil)
	assertEqual(t, string(data), string(sample1[:])+string(sample2[:]))
	assert(t, r1.Close() == nil)
}

func assertEqual(t testing.TB, val1, val2 string) {
	if val1 != val2 {
		t.Fatalf("val1 (len %d) != val2 (len %d)", len(val1), len(val2))
	}
}
