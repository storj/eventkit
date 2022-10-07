package delimited

import (
	"bytes"
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

func assertNoErr(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDelimitedBasic(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	w := NewWriter(&out)
	_, err := w.Write([]byte("hello, "))
	assert(t, err == nil)
	_, err = w.Write([]byte("friend"))
	assert(t, err == nil)
	assert(t, w.Flush() == nil)

	assertEqual(t, readAll(NewReader(&out)),
		[]string{"hello, friend"})
}

func TestDelimitedFramed(t *testing.T) {
	t.Parallel()

	var sample [65536 + 10]byte
	rand.Read(sample[:])

	var out bytes.Buffer
	w := NewWriter(&out)
	_, err := w.Write(sample[:])
	assert(t, err == nil)
	_, err = w.Write(sample[:])
	assert(t, err == nil)
	_, err = w.Write(sample[:])
	assert(t, err == nil)
	assert(t, w.Flush() == nil)

	assertEqual(t, readAll(NewReader(&out)),
		[]string{string(sample[:]) + string(sample[:]) + string(sample[:])})
}

func readAll(r *Reader) []string {
	var rv []string
	for r.Next() {
		data, err := io.ReadAll(r)
		if err != nil {
			panic(err)
		}
		rv = append(rv, string(data))
	}
	err := r.Err()
	if err != nil {
		panic(err)
	}
	return rv
}

func assertEqual(t testing.TB, a, b []string) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("lengths of %d and %d unequal", len(a), len(b))
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			t.Fatalf("index %q of %q and %q unequal", i, a[i], b[i])
		}
	}
}

func TestDelimitedEdges(t *testing.T) {
	t.Parallel()

	var sample [65536 + 10]byte
	rand.Read(sample[:])

	var out bytes.Buffer
	w := NewWriter(&out)
	// empty
	assert(t, w.Delimit() == nil)
	_, err := w.Write(sample[:])
	assert(t, err == nil)
	_, err = w.Write(sample[:])
	assert(t, err == nil)
	_, err = w.Write(sample[:])
	assert(t, err == nil)
	// sample + sample + sample
	assert(t, w.Delimit() == nil)
	// sample
	_, err = w.Write(sample[:])
	assert(t, err == nil)
	assert(t, w.Delimit() == nil)
	// empty
	assert(t, w.Delimit() == nil)
	_, err = w.Write(sample[:])
	assert(t, err == nil)
	_, err = w.Write(sample[:])
	assert(t, err == nil)
	// sample + sample
	assert(t, w.Delimit() == nil)
	// empty
	assert(t, w.Delimit() == nil)
	// empty

	data := readAll(NewReader(&out))
	assertEqual(t, data, []string{
		"",
		string(sample[:]) + string(sample[:]) + string(sample[:]),
		string(sample[:]),
		"",
		string(sample[:]) + string(sample[:]),
		"",
		"",
	})
}
