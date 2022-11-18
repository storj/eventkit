package main

import (
	"fmt"
	tea "github.com/charmbracelet/bubbletea"
	ui "github.com/elek/bubbles"
	"sort"
	"strings"
)

func NewTop(repo *Repo) tea.Model {
	scopes := NewScopes(repo)
	names := NewNames(repo)
	tags := NewTags(repo)
	values := NewValues(repo)
	horizontal := &ui.Horizontal{}
	horizontal.Add(scopes, ui.FixedSize(30))
	horizontal.Add(names, ui.FixedSize(30))
	horizontal.Add(tags, ui.FixedSize(30))
	horizontal.Add(ui.WithBorder(values), ui.RemainingSize())
	fg := ui.NewFocusGroup(horizontal)
	fg.Add(scopes)
	fg.Add(names)
	fg.Add(tags)
	return fg
}

type counts []count

type count struct {
	class string
	name  string
	count int
}

func countsFromMap(name string, c map[string]int) (res counts) {
	for k, v := range c {
		res = append(res, count{
			class: name,
			name:  k,
			count: v,
		})
	}
	return res
}

func (c counts) doSort() {
	sort.Slice(c, func(i, j int) bool {
		if c[i].count == c[j].count {
			return strings.Compare(c[i].name, c[j].name) < 1
		}
		return c[i].count > c[j].count
	})
}

type Scopes struct {
	*ui.List[count]
	repo *Repo
}

func NewScopes(repo *Repo) tea.Model {
	return &Scopes{
		repo: repo,
		List: ui.NewList[count]([]count{}, func(a count) string {
			return fmt.Sprintf("%s %s", Colorized(fmt.Sprintf("%-4d", a.count), Red), a.name)
		}),
	}
}

func (l *Scopes) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg.(type) {
	case ui.RefreshMsg:
		var oldValue count
		if !l.List.Empty() {
			oldValue = l.List.Selected()
		}

		var newValues = countsFromMap("scope", l.repo.GetScopes())
		newValues.doSort()
		l.SetContent(newValues)
		reselect(newValues, oldValue, l.List)
	}
	m, c := l.List.Update(msg)
	l.List = m.(*ui.List[count])
	return l, c
}

func reselect(newValues counts, oldValue count, l *ui.List[count]) {
	for ix, r := range newValues {
		if r.name == oldValue.name {
			l.Select(ix)
			break
		}
	}
}

type Names struct {
	*ui.List[count]
	names map[string]int
	scope string
	repo  *Repo
}

func NewNames(repo *Repo) tea.Model {
	return &Names{
		repo:  repo,
		names: map[string]int{},
		List: ui.NewList([]count{}, func(a count) string {
			return fmt.Sprintf("%s %s", Colorized(fmt.Sprintf("%-4d", a.count), Red), a.name)
		}),
	}
}
func (l *Names) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case ui.RefreshMsg:
		if l.repo.Count() == 0 {
			return l, nil
		}
		if l.scope == "" {
			l.scope = l.repo.Events[0].ScopeStr()
		}

		var oldValue count
		if !l.List.Empty() {
			oldValue = l.List.Selected()
		}

		var newValues = countsFromMap("name", l.repo.GetNames(l.scope))
		newValues.doSort()
		l.SetContent(newValues)
		reselect(newValues, oldValue, l.List)

	case ui.FocusedItemMsg[count]:
		switch msg.Item.class {
		case "scope":
			l.scope = msg.Item.name
			l.List.Reset()
		}
	}
	m, c := l.List.Update(msg)
	l.List = m.(*ui.List[count])
	return l, c
}

type Tags struct {
	*ui.List[count]
	scope string
	name  string
	repo  *Repo
}

func NewTags(repo *Repo) tea.Model {
	return &Tags{
		repo: repo,
		List: ui.NewList([]count{}, func(a count) string {
			return fmt.Sprintf("%s %s", Colorized(fmt.Sprintf("%-4d", a.count), Red), a.name)
		}),
	}
}
func (l *Tags) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case ui.RefreshMsg:
		if l.repo.Count() == 0 {
			return l, nil
		}
		if l.scope == "" {
			l.scope = l.repo.Events[0].ScopeStr()
			l.name = l.repo.Events[0].Event.Name
		}

		var oldValue count
		if !l.List.Empty() {
			oldValue = l.List.Selected()
		}

		var newValues = countsFromMap("tag", l.repo.GetTags(l.scope, l.name))
		newValues.doSort()
		l.SetContent(newValues)
		reselect(newValues, oldValue, l.List)
	case ui.FocusedItemMsg[count]:
		switch msg.Item.class {
		case "scope":
			l.scope = msg.Item.name
			l.List.Reset()
		case "name":
			l.name = msg.Item.name
			l.List.Reset()
		}
	}
	m, c := l.List.Update(msg)
	l.List = m.(*ui.List[count])
	return l, c
}

type Values struct {
	*ui.Text
	scope string
	name  string
	tag   string
	repo  *Repo
}

func NewValues(repo *Repo) tea.Model {
	v := &Values{
		repo: repo,
	}
	v.Text = ui.NewText(v.render)
	return v
}

func (l *Values) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case ui.RefreshMsg:
		if l.repo.Count() == 0 {
			return l, nil
		}
		if l.scope == "" {
			l.scope = l.repo.Events[0].ScopeStr()
			l.name = l.repo.Events[0].Event.Name
			if l.repo.Events[0].Event.Tags != nil && len(l.repo.Events[0].Event.Tags) > 0 {
				l.tag = l.repo.Events[0].Event.Tags[0].Key
			}

		}
	case ui.FocusedItemMsg[count]:
		switch msg.Item.class {
		case "scope":
			l.scope = msg.Item.name
		case "name":
			l.name = msg.Item.name
		case "tag":
			l.tag = msg.Item.name
		}

	}
	m, c := l.Text.Update(msg)
	l.Text = m.(*ui.Text)
	return l, c
}

func (l *Values) render() string {
	out := ""
	counts := countsFromMap("value", l.repo.GetCounts(l.scope, l.name, l.tag))
	counts.doSort()

	for _, c := range counts {
		out += fmt.Sprintf("%s %s\n", Colorized(fmt.Sprintf("%5d", c.count), Red), c.name)
	}

	return out
}
