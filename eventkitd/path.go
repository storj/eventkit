package main

import (
	"encoding/hex"
	"strings"
	"time"
)

func pathEscapeTo(val string, out *strings.Builder) {
	for _, b := range []byte(val) {
		switch {
		case b >= 'a' && b <= 'z':
			out.WriteByte(b)
		case b >= 'A' && b <= 'Z':
			out.WriteByte(b)
		case b >= '0' && b <= '9':
			out.WriteByte(b)
		case b == '-':
			out.WriteByte(b)
		case b == '_':
			out.WriteString("__")
		default:
			buf := [3]byte{'_', 0, 0}
			hex.Encode(buf[1:], []byte{b})
			out.Write(buf[:])
		}
	}
}

func computePath(base string, timestamp time.Time, scope []string, name string) string {
	var out strings.Builder
	out.WriteString(base)
	out.WriteString(timestamp.Format("2006-01/02-15/"))
	for i, s := range scope {
		if i != 0 {
			out.WriteString("_-")
		}
		pathEscapeTo(s, &out)
	}
	out.WriteByte('/')
	pathEscapeTo(name, &out)
	return out.String()
}
