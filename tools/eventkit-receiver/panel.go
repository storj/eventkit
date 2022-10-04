package main

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	ui "github.com/elek/bubbles"
)

var (
	Green  = lipgloss.Color("#01a252")
	Yellow = lipgloss.Color("#fded02")
	Red    = lipgloss.Color("#db2d20")
)

func Colorized(orig string, color lipgloss.Color) string {
	return lipgloss.NewStyle().Foreground(color).Render(orig)
}

type MainPane struct {
	*ui.Tabs
	events []*Event
	repo   *Repo
}

func NewPanel() tea.Model {

	m := &MainPane{
		events: []*Event{},
		repo:   NewRepo(),
	}

	log := ui.Tab{
		Name:  "log",
		Model: ui.WithBorder(NewRawLog(m.repo)),
		Key:   "l",
	}
	top := ui.Tab{
		Name:  "top",
		Model: NewTop(m.repo),
		Key:   "t",
	}
	m.Tabs = ui.NewTabs(top, log)

	return m
}

func (l *MainPane) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case *Packet:
		var cmd tea.Cmd
		for ix := range msg.Packet.Events {
			event := Event{
				Event:      msg.Packet.Events[ix],
				Source:     msg.Source,
				ReceivedAt: msg.ReceivedAt,
			}
			l.repo.Add(&event)
			m, c := l.Tabs.UpdateAll(ui.RefreshMsg{})
			l.Tabs = m.(*ui.Tabs)
			cmd = tea.Batch(c, cmd)
		}
		return l, cmd
	}

	m, c := l.Tabs.Update(msg)
	l.Tabs = m.(*ui.Tabs)
	return l, c
}
