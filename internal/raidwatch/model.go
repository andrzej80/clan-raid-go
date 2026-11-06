// Package raidwatch — терминальный клиент для WatchRaid на bubbletea.
//
// Состояние экрана лежит в Model, все изменения идут через Update, отрисовка — в View.
// Сетевой код в Model не попадает: он присылает в программу сообщения tea.Msg.
package raidwatch

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("212"))

// Model — состояние экрана raidwatch.
type Model struct {
	raidID string
}

// New создаёт модель для рейда raidID.
func New(raidID string) Model {
	return Model{raidID: raidID}
}

// Init ничего не запускает: сетевые горутины стартуют снаружи и шлют сообщения через Program.Send.
func (m Model) Init() tea.Cmd { return nil }

// Update пока обрабатывает только выход; остальные сообщения появятся в следующих частях.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok {
		switch key.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		}
	}
	return m, nil
}

// View рисует экран.
func (m Model) View() string {
	return titleStyle.Render("raidwatch") + fmt.Sprintf(" — рейд %s\n\nq или Ctrl+C — выход\n", m.raidID)
}
