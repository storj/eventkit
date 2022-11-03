package main

import "fmt"

// Repo stores all events in-memory with counters.
type Repo struct {
	Events   []*Event
	Counters []*Counter
}

type Counter struct {
	Scope string
	Name  string
	Tag   string
	Count int
}

func NewRepo() *Repo {
	return &Repo{
		Events:   []*Event{},
		Counters: []*Counter{},
	}
}

func (r *Repo) Add(e *Event) {
	r.Events = append(r.Events, e)
}

func (r *Repo) GetScopes() map[string]int {
	res := map[string]int{}
	for _, e := range r.Events {
		res[e.ScopeStr()]++
	}
	return res
}

func (r *Repo) GetNames(scope string) map[string]int {
	res := map[string]int{}
	for _, e := range r.Events {
		if e.ScopeStr() == scope {
			res[e.Event.Name]++
		}
	}
	return res
}

func (r *Repo) GetTags(scope string, name string) map[string]int {
	res := map[string]int{}
	for _, e := range r.Events {
		if e.ScopeStr() == scope && e.Event.Name == name {
			for _, t := range e.Event.Tags {
				res[t.Key]++
			}
		}
	}
	return res
}

func (r *Repo) GetCounts(scope string, name string, tag string) map[string]int {
	res := map[string]int{}
	for _, e := range r.Events {
		if e.ScopeStr() == scope && e.Event.Name == name {
			for _, t := range e.Event.Tags {
				if t.Key == tag {
					res[fmt.Sprintf("%s", t.Value)]++
				}
			}
		}
	}
	return res
}

func (r *Repo) Count() int {
	return len(r.Events)
}
