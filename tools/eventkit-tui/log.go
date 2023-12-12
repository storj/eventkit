package main

import (
	"fmt"
	"strings"

	ui "github.com/elek/bubbles"
)

type RawLog struct {
	*ui.Text
	repo  *Repo
	start int
}

func NewRawLog(repo *Repo) *RawLog {
	r := &RawLog{
		repo: repo,
	}
	r.Text = ui.NewText(r.render)
	return r
}

func (l *RawLog) render() string {
	out := ""

	for l.start+l.Text.GetHeight() <= len(l.repo.Events) && l.Text.GetHeight() > 5 {
		l.start = l.start + l.Text.GetHeight() - 5
	}

	for i := l.start; i < l.start+l.Text.GetHeight(); i++ {
		if len(l.repo.Events)-1 < i {
			break
		}
		e := l.repo.Events[i]
		var tags []string
		for _, t := range e.Event.Tags {
			tags = append(tags, fmt.Sprintf("%s=%s", t.Key, t.ValueString()))
		}
		d := Colorized(fmt.Sprintf("%15s", e.ReceivedAt.Format("2006-01-02 15:04:05")), Green)
		out += fmt.Sprintf("%s %s %s %s\n", d, strings.Join(e.Event.Scope, "."), Colorized(e.Event.Name, Yellow), strings.Join(tags, " "))
	}
	return out
}
