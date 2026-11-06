package raidwatch

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

// apply прогоняет сообщения через Update, как это делает bubbletea.
func apply(m Model, msgs ...tea.Msg) Model {
	for _, msg := range msgs {
		next, _ := m.Update(msg)
		m = next.(Model)
	}
	return m
}

func TestGroupDigits(t *testing.T) {
	t.Parallel()
	for in, want := range map[int64]string{
		0: "0", 999: "999", 1000: "1 000", 1234567: "1 234 567", -1234: "-1 234",
	} {
		if got := groupDigits(in); got != want {
			t.Errorf("groupDigits(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestPercentRoundsUp(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		left, total int64
		want        int
	}{{0, 100, 0}, {1, 1000, 1}, {50, 100, 50}, {100, 100, 100}, {5, 0, 0}, {200, 100, 100}} {
		if got := percent(tc.left, tc.total); got != tc.want {
			t.Errorf("percent(%d, %d) = %d, want %d", tc.left, tc.total, got, tc.want)
		}
	}
}

func TestHPBar(t *testing.T) {
	t.Parallel()
	count := func(s string) (full, empty int) {
		return strings.Count(s, "█"), strings.Count(s, "░")
	}
	for _, tc := range []struct {
		name        string
		frac        float64
		full, empty int
	}{
		{"полная", 1, 10, 0},
		{"пустая", 0, 0, 10},
		{"половина", 0.5, 5, 5},
		{"почти пустая не выглядит пустой", 0.001, 1, 9},
		{"почти полная не выглядит полной", 0.999, 9, 1},
		{"за границами", 2, 10, 0},
		{"отрицательная", -1, 0, 10},
	} {
		full, empty := count(hpBar(tc.frac, 10))
		if full != tc.full || empty != tc.empty {
			t.Errorf("%s: bar = %d/%d, want %d/%d", tc.name, full, empty, tc.full, tc.empty)
		}
	}
}

func TestView_BeforeSnapshot(t *testing.T) {
	t.Parallel()
	out := New("raid-1").View()
	for _, want := range []string{"raidwatch", "подключение", "Ждём состояние рейда", "q или Ctrl+C"} {
		if !strings.Contains(out, want) {
			t.Errorf("view does not contain %q:\n%s", want, out)
		}
	}
}

func TestView_LiveRaid(t *testing.T) {
	t.Parallel()
	m := apply(New("raid-1"),
		tea.WindowSizeMsg{Width: 100, Height: 30},
		SnapshotMsg{BossName: "Дракон", HPMax: 5000, HPLeft: 1234, Status: raidbossV1.RaidStatus_RAID_STATUS_ACTIVE},
		HitsMsg{HitsTotal: 42},
		LeaderboardMsg{Entries: []Entry{{PlayerID: 17, Damage: 12345}, {PlayerID: 4, Damage: 900}}},
	)
	out := m.View()
	for _, want := range []string{
		"Дракон", "● live", "1 234 / 5 000", "(25%)", "Ударов: 42", "игрок #17", "12 345", "игрок #4",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("view does not contain %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "добил") || strings.Contains(out, "повержен") {
		t.Errorf("unfinished raid shows the final banner:\n%s", out)
	}
}

func TestView_HitsUnknownUntilFirstUpdate(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), SnapshotMsg{HPMax: 100, HPLeft: 100})
	if out := m.View(); !strings.Contains(out, "Ударов: —") {
		t.Errorf("view before first hits update:\n%s", out)
	}
}

func TestView_Reconnecting(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), SnapshotMsg{HPMax: 100, HPLeft: 50}, ConnMsg{State: ConnReconnecting})
	if out := m.View(); !strings.Contains(out, "переподключение") {
		t.Errorf("view while reconnecting:\n%s", out)
	}
	m = apply(m, SnapshotMsg{HPMax: 100, HPLeft: 40}) // новый snapshot возвращает live
	if out := m.View(); !strings.Contains(out, "● live") || strings.Contains(out, "переподключение") {
		t.Errorf("view after reconnect:\n%s", out)
	}
}

func TestView_FinishedDefeated(t *testing.T) {
	t.Parallel()
	m := apply(New("r"),
		SnapshotMsg{HPMax: 100, HPLeft: 10},
		LeaderboardMsg{Entries: []Entry{{PlayerID: 7, Damage: 90}, {PlayerID: 3, Damage: 10}}},
		FinishedMsg{Status: raidbossV1.RaidStatus_RAID_STATUS_DEFEATED, KillerPlayerID: 7},
	)
	out := m.View()
	for _, want := range []string{
		"Босс повержен", "Добил игрок #7", "← добил", "○ завершён", "игрок #3",
		"0 / 100 (0%)", // последний hp_left мог не дойти, но у побеждённого босса HP 0
	} {
		if !strings.Contains(out, want) {
			t.Errorf("view does not contain %q:\n%s", want, out)
		}
	}
}

func TestView_FinishedExpired(t *testing.T) {
	t.Parallel()
	m := apply(New("r"),
		SnapshotMsg{HPMax: 100, HPLeft: 60},
		FinishedMsg{Status: raidbossV1.RaidStatus_RAID_STATUS_EXPIRED},
	)
	out := m.View()
	if !strings.Contains(out, "Время вышло") || strings.Contains(out, "← добил") {
		t.Errorf("view of expired raid:\n%s", out)
	}
}

func TestView_FinishedWithoutLeaderboard(t *testing.T) {
	t.Parallel()
	// Рейд закончился до подключения: сервер присылает snapshot и finished без лидерборда.
	m := apply(New("r"), SnapshotMsg{HPMax: 100}, FinishedMsg{Status: raidbossV1.RaidStatus_RAID_STATUS_EXPIRED})
	out := m.View()
	if !strings.Contains(out, "лидерборд недоступен") || strings.Contains(out, "никто не ударил") {
		t.Errorf("view:\n%s", out)
	}
}

func TestView_NarrowTerminalKeepsMinimumBar(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), tea.WindowSizeMsg{Width: 20}, SnapshotMsg{HPMax: 100, HPLeft: 100})
	if got := strings.Count(m.View(), "█"); got != minBarWidth {
		t.Errorf("bar width = %d, want %d", got, minBarWidth)
	}
}

// Строка HP не должна быть шире терминала: иначе её приходится обрезать или переносить,
// и перерисовка кадра ломается. Проверяем самые длинные числа (9 знаков с разделителями).
func TestView_HPLineFitsTerminalWidth(t *testing.T) {
	t.Parallel()
	for width := 60; width <= 200; width++ {
		m := apply(New("r"), tea.WindowSizeMsg{Width: width, Height: 40},
			SnapshotMsg{HPMax: 100_000_000, HPLeft: 100_000_000})
		if got := lipgloss.Width(m.hpLine()); got > width {
			t.Fatalf("width %d: hp line is %d wide:\n%s", width, got, m.hpLine())
		}
	}
}

func lineCount(s string) int {
	return strings.Count(strings.TrimRight(s, "\n"), "\n") + 1
}

// Кадр не выше окна: иначе в обычном режиме верхние строки уходят в прокрутку и дублируются.
func TestView_FrameFitsTerminalHeight(t *testing.T) {
	t.Parallel()
	top := make([]Entry, topSize)
	for i := range top {
		top[i] = Entry{PlayerID: int64(i + 1), Damage: int64(1000 - i)}
	}
	for height := 12; height <= 30; height++ {
		live := apply(New("r"), tea.WindowSizeMsg{Width: 100, Height: height},
			SnapshotMsg{HPMax: 100, HPLeft: 50}, LeaderboardMsg{Entries: top})
		if got := lineCount(live.View()); got >= height {
			t.Fatalf("height %d: live frame has %d lines", height, got)
		}
		done := apply(live, FinishedMsg{Status: raidbossV1.RaidStatus_RAID_STATUS_EXPIRED})
		if got := lineCount(done.View()); got >= height {
			t.Fatalf("height %d: final frame has %d lines", height, got)
		}
	}
}

func TestView_ShowsAllTopRowsWhenTall(t *testing.T) {
	t.Parallel()
	top := make([]Entry, topSize)
	for i := range top {
		top[i] = Entry{PlayerID: int64(i + 1), Damage: 100}
	}
	m := apply(New("r"), tea.WindowSizeMsg{Width: 100, Height: 50},
		SnapshotMsg{HPMax: 100, HPLeft: 50}, LeaderboardMsg{Entries: top})
	if got := strings.Count(m.View(), "игрок #"); got != topSize {
		t.Fatalf("rows = %d, want %d", got, topSize)
	}
}
