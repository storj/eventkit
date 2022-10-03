package delimited

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func assert(val bool) {
	if !val {
		panic("assertion failed")
	}
}

func TestDelimitedBasic(t *testing.T) {
	var out bytes.Buffer
	w := NewWriter(&out)
	_, err := w.Write([]byte("hello, "))
	assert(err == nil)
	_, err = w.Write([]byte("friend"))
	assert(err == nil)
	assert(w.Flush() == nil)

	r := NewReader(&out)
	data, err := io.ReadAll(r)
	assert(err == nil)
	assert(string(data) == "hello, friend")
	assert(!r.Delimited())
}

func TestDelimitedFramed(t *testing.T) {
	var sample [65536 + 10]byte
	rand.Read(sample[:])

	var out bytes.Buffer
	w := NewWriter(&out)
	_, err := w.Write(sample[:])
	assert(err == nil)
	_, err = w.Write(sample[:])
	assert(err == nil)
	_, err = w.Write(sample[:])
	assert(err == nil)
	assert(w.Flush() == nil)

	r := NewReader(&out)
	data, err := io.ReadAll(r)
	assert(err == nil)
	assert(string(data) == string(sample[:])+string(sample[:])+string(sample[:]))
	assert(!r.Delimited())
}

func readAll(r *Reader) []string {
	var rv []string
	for {
		data, err := io.ReadAll(r)
		if err != nil {
			panic(err)
		}
		rv = append(rv, string(data))
		if !r.Delimited() {
			return rv
		}
		r.Advance()
	}
}

func assertEqual(a, b []string) {
	assert(len(a) == len(b))
	for i := 0; i < len(a); i++ {
		assert(a[i] == b[i])
	}
}

func TestDelimitedEdges(t *testing.T) {
	var sample [65536 + 10]byte
	rand.Read(sample[:])

	var out bytes.Buffer
	w := NewWriter(&out)
	// empty
	assert(w.Delimit() == nil)
	_, err := w.Write(sample[:])
	assert(err == nil)
	_, err = w.Write(sample[:])
	assert(err == nil)
	_, err = w.Write(sample[:])
	assert(err == nil)
	// sample + sample + sample
	assert(w.Delimit() == nil)
	// sample
	_, err = w.Write(sample[:])
	assert(err == nil)
	assert(w.Delimit() == nil)
	// empty
	assert(w.Delimit() == nil)
	_, err = w.Write(sample[:])
	assert(err == nil)
	_, err = w.Write(sample[:])
	assert(err == nil)
	// sample + sample
	assert(w.Delimit() == nil)
	// empty
	assert(w.Delimit() == nil)
	// empty

	data := readAll(NewReader(&out))
	assertEqual(data, []string{
		"",
		string(sample[:]) + string(sample[:]) + string(sample[:]),
		string(sample[:]),
		"",
		string(sample[:]) + string(sample[:]),
		"",
		"",
	})
}
