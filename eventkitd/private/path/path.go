package path

import (
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

func EscapeTo(val string, out *strings.Builder) {
	for _, b := range []byte(val) {
		switch {
		case b >= 'a' && b <= 'z':
			out.WriteByte(b)
		case b >= 'A' && b <= 'Z':
			out.WriteByte('+')
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

func Unescape(val string) (string, error) {
	bytes := []byte(val)
	var out strings.Builder
nextChar:
	for len(bytes) > 0 {
		b := bytes[0]
		switch {
		case (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
			(b >= '0' && b <= '9') || (b == '-'):
			out.WriteString(strings.ToLower(string(b)))
			bytes = bytes[1:]
			continue nextChar
		case b == '+':
			if len(bytes) < 2 {
				return "", fmt.Errorf("%q unparsable", val)
			}
			c := bytes[1]
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
				out.WriteString(strings.ToUpper(string(c)))
				bytes = bytes[2:]
				continue nextChar
			}
			return "", fmt.Errorf("%q unparsable", val)
		case b == '_':
			if len(bytes) > 1 && bytes[1] == '_' {
				out.WriteByte('_')
				bytes = bytes[2:]
				continue nextChar
			}
			if len(bytes) < 3 {
				return "", fmt.Errorf("%q unparsable", val)
			}
			res, err := hex.DecodeString(string(bytes[1:][:2]))
			if err != nil {
				return "", err
			}
			out.WriteByte(res[0])
			bytes = bytes[3:]
			continue nextChar
		default:
			return "", fmt.Errorf("%q unparsable", val)
		}
	}
	return out.String(), nil
}

const dateFormat = "2006-01" + string(filepath.Separator) + "02-15" + string(filepath.Separator)

func EncodeScope(scope []string) string {
	var out strings.Builder
	for i, s := range scope {
		if i != 0 {
			out.WriteString("_-")
		}
		EscapeTo(s, &out)
	}
	return out.String()
}

func Compute(base string, timestamp time.Time, scope []string, name string) string {
	var out strings.Builder
	out.WriteString(base)
	out.WriteString(timestamp.Format(dateFormat))
	for i, s := range scope {
		if i != 0 {
			out.WriteString("_-")
		}
		EscapeTo(s, &out)
	}
	out.WriteByte(filepath.Separator)
	EscapeTo(name, &out)
	return out.String()
}

func Parse(path string) (name string, scope []string, err error) {
	name, err = Unescape(filepath.Base(path))
	if err != nil {
		return "", nil, err
	}
	parts := strings.Split(filepath.Base(filepath.Dir(path)), "_-")
	scope = make([]string, 0, len(parts))
	for _, part := range parts {
		part, err = Unescape(part)
		if err != nil {
			return "", nil, err
		}
		scope = append(scope, part)
	}
	return name, scope, nil
}
