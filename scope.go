package eventkit

import (
	"time"
)

type Scope struct {
	r    *Registry
	name []string
}

func (s *Scope) Subscope(name string) *Scope {
	return &Scope{r: s.r, name: append(append([]string(nil), s.name...), name)}
}

func (s *Scope) Event(name string, tags ...interface{}) {
	em := make(EventMap, len(tags)/2+3)
	if len(tags)%2 != 0 {
		panic("tag name/value pairs unmatched")
	}
	for i := 0; i < len(tags); i += 2 {
		em[tags[i].(string)] = tags[i+1]
	}
	em["name"] = name
	em["scope"] = s.name
	em["timestamp"] = time.Now()
	s.r.Submit(em)
}
