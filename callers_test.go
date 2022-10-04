package eventkit

import (
	"github.com/zeebo/assert"
	"testing"
)

func TestCallerPackage(t *testing.T) {
	assert.Equal(t, "/testing", callerPackage(1))
	assert.Equal(t, "/runtime", callerPackage(2))

}
