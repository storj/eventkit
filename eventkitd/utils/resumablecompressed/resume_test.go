package resumablecompressed

import (
	"bytes"
	"compress/zlib"
	"crypto/rand"
	"io"
	"testing"
)

func assert(val bool) {
	if !val {
		panic("assertion failed")
	}
}

type writeNopCloser struct {
	io.Writer
}

func (w writeNopCloser) Close() error { return nil }

func TestResumable(t *testing.T) {
	var sample1 [655360]byte
	rand.Read(sample1[:])
	var sample2 [655360]byte
	rand.Read(sample2[:])

	var out bytes.Buffer

	w1, err := NewWriter(writeNopCloser{Writer: &out}, zlib.DefaultCompression)
	assert(err == nil)
	_, err = w1.Write(sample1[:])
	assert(err == nil)
	assert(w1.Close() == nil)

	w2, err := NewWriter(writeNopCloser{Writer: &out}, zlib.DefaultCompression)
	assert(err == nil)
	_, err = w2.Write(sample2[:])
	assert(err == nil)
	assert(w2.Close() == nil)

	r1 := NewReader(&out)
	assert(err == nil)
	data, err := io.ReadAll(r1)
	assert(err == nil)
	assert(string(data) == string(sample1[:])+string(sample2[:]))
	assert(r1.Close() == nil)
}
