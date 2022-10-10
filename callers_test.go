package eventkit

import (
	"testing"
)

func assertEqual(t *testing.T, a, b interface{}) {
	t.Helper()
	if a != b {
		t.Fatalf("%q != %q", a, b)
	}
}

func TestCallerPackage(t *testing.T) {
	assertEqual(t, "/testing", callerPackage(1))
	assertEqual(t, "/runtime", callerPackage(2))

}
