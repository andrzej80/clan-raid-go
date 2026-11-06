package raidwatch

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/lipgloss"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

const (
	defaultWidth = 80 // пока терминал не сообщил свой размер
	minBarWidth  = 10
	maxBarWidth  = 60
	barReserve   = 32 // место справа от полоски: подпись «1 234 567 / 5 000 000 (100%)»
)

var (
	titleStyle  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("212"))
	dimStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("244"))
	okStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	warnStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	badStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	headerStyle = lipgloss.NewStyle().Bold(true)
)

// View рисует экран: пока рейд идёт — полоска HP, скорость и топ, после финала — итог.
func (m Model) View() string {
	var b strings.Builder
	b.WriteString(m.header())
	b.WriteString("\n\n")

	if !m.haveSnapshot {
		b.WriteString(dimStyle.Render("Ждём состояние рейда…"))
		b.WriteString("\n\n")
		b.WriteString(dimStyle.Render("q или Ctrl+C — выход"))
		b.WriteString("\n")
		return b.String()
	}

	if m.finished {
		b.WriteString(m.finalBanner())
		b.WriteString("\n\n")
	}
	b.WriteString(m.hpLine())
	b.WriteString("\n")
	b.WriteString(m.ratesLine())
	b.WriteString("\n\n")
	b.WriteString(m.topTable())
	b.WriteString("\n")
	b.WriteString(dimStyle.Render("q или Ctrl+C — выход"))
	b.WriteString("\n")
	return b.String()
}

func (m Model) header() string {
	name := m.bossName
	if name == "" {
		name = "рейд " + m.raidID
	}
	return titleStyle.Render("raidwatch") + dimStyle.Render(" · ") + name + "  " + m.connLabel()
}

func (m Model) connLabel() string {
	if m.finished {
		return dimStyle.Render("○ завершён")
	}
	switch m.conn {
	case ConnLive:
		return okStyle.Render("● live")
	case ConnReconnecting:
		return warnStyle.Render("◌ переподключение…")
	case ConnConnecting:
		return warnStyle.Render("◌ подключение…")
	}
	return warnStyle.Render("◌ подключение…")
}

func (m Model) hpLine() string {
	width := m.width
	if width <= 0 {
		width = defaultWidth
	}
	barWidth := min(max(width-barReserve, minBarWidth), maxBarWidth)

	frac := 0.0
	if m.hpMax > 0 {
		frac = float64(m.hpLeft) / float64(m.hpMax)
	}
	return fmt.Sprintf("HP  %s  %s / %s (%d%%)",
		hpBar(frac, barWidth), groupDigits(m.hpLeft), groupDigits(m.hpMax), percent(m.hpLeft, m.hpMax))
}

// hpBar рисует полоску из width ячеек; цвет зависит от остатка HP.
func hpBar(frac float64, width int) string {
	frac = min(max(frac, 0), 1)
	filled := int(frac*float64(width) + 0.5)
	// Ненулевое HP не должно выглядеть пустой полоской, а нулевое — полной.
	if frac > 0 && filled == 0 {
		filled = 1
	}
	if frac < 1 && filled == width {
		filled = width - 1
	}
	style := okStyle
	switch {
	case frac <= 0.2:
		style = badStyle
	case frac <= 0.5:
		style = warnStyle
	}
	return style.Render(strings.Repeat("█", filled)) + dimStyle.Render(strings.Repeat("░", width-filled))
}

func percent(left, total int64) int {
	if total <= 0 {
		return 0
	}
	p := int((left*100 + total - 1) / total) // вверх: 1% остатка не округляется до 0
	return min(max(p, 0), 100)
}

func (m Model) ratesLine() string {
	dps, hps := m.Rates()
	hits := "—"
	if m.hitsKnown {
		hits = groupDigits(m.hitsTotal)
	}
	return fmt.Sprintf("Ударов: %s · %s/с    Урон: %s/с", hits, groupDigits(hps), groupDigits(dps))
}

func (m Model) topTable() string {
	var b strings.Builder
	b.WriteString(headerStyle.Render("Топ игроков"))
	b.WriteString("\n")
	if len(m.top) == 0 {
		b.WriteString(dimStyle.Render("  пока никто не ударил"))
		b.WriteString("\n")
		return b.String()
	}
	for i, e := range m.top {
		killer := ""
		if m.finished && e.PlayerID == m.killerPlayerID && m.finalStatus == raidbossV1.RaidStatus_RAID_STATUS_DEFEATED {
			killer = badStyle.Render("  ← добил")
		}
		fmt.Fprintf(&b, "%3d  игрок #%-8d %12s%s\n", i+1, e.PlayerID, groupDigits(e.Damage), killer)
	}
	return b.String()
}

func (m Model) finalBanner() string {
	switch m.finalStatus {
	case raidbossV1.RaidStatus_RAID_STATUS_DEFEATED:
		return okStyle.Render("Босс повержен!") + fmt.Sprintf(" Добил игрок #%d.", m.killerPlayerID)
	case raidbossV1.RaidStatus_RAID_STATUS_EXPIRED:
		return warnStyle.Render("Время вышло: босс выстоял.")
	case raidbossV1.RaidStatus_RAID_STATUS_UNSPECIFIED, raidbossV1.RaidStatus_RAID_STATUS_ACTIVE:
	}
	return dimStyle.Render("Рейд завершён.")
}

// groupDigits разделяет разряды пробелами: 1234567 → «1 234 567».
func groupDigits(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(' ')
		}
		b.WriteRune(r)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}
