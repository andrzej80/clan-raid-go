package raidwatch

import (
	"errors"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

var tickMsg = TickMsg(time.Time{})

func snapshotMsg(hpMax, hpLeft int64) SnapshotMsg {
	return SnapshotMsg{BossName: "boss", HPMax: hpMax, HPLeft: hpLeft, Status: raidbossV1.RaidStatus_RAID_STATUS_ACTIVE}
}

// update применяет одно сообщение и возвращает и модель, и команду.
func update(m Model, msg tea.Msg) (Model, tea.Cmd) {
	next, cmd := m.Update(msg)
	return next.(Model), cmd
}

func rates(t *testing.T, m Model) (dps, hps int64) {
	t.Helper()
	return m.Rates()
}

func TestUpdate_SnapshotSetsState(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), snapshotMsg(1000, 700))
	if !m.haveSnapshot || m.bossName != "boss" || m.hpMax != 1000 || m.hpLeft != 700 || m.conn != ConnLive {
		t.Fatalf("model after snapshot = %+v", m)
	}
}

func TestUpdate_SnapshotClampsHP(t *testing.T) {
	t.Parallel()
	if m := apply(New("r"), snapshotMsg(100, 500)); m.hpLeft != 100 {
		t.Errorf("hp above max = %d, want 100", m.hpLeft)
	}
	if m := apply(New("r"), snapshotMsg(100, -5)); m.hpLeft != 0 {
		t.Errorf("negative hp = %d, want 0", m.hpLeft)
	}
}

func TestUpdate_UpdatesBeforeSnapshotAreIgnored(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), HPMsg{HPLeft: 10}, HitsMsg{HitsTotal: 5})
	if m.hpLeft != 0 || m.hitsTotal != 0 || m.hitsKnown || m.pending != (sample{}) {
		t.Fatalf("model reacted to updates without a snapshot: %+v", m)
	}
}

func TestUpdate_HPDropCountsAsDamage(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), snapshotMsg(1000, 1000), HPMsg{HPLeft: 900}, HPMsg{HPLeft: 700}, tickMsg)
	if m.hpLeft != 700 {
		t.Fatalf("hp = %d, want 700", m.hpLeft)
	}
	if dps, _ := rates(t, m); dps != 300 {
		t.Fatalf("dps = %d, want 300 (damage between ticks adds up)", dps)
	}
}

func TestUpdate_HPIncreaseIsAcceptedButNotCounted(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), snapshotMsg(1000, 500), HPMsg{HPLeft: 800}, tickMsg)
	if m.hpLeft != 800 {
		t.Fatalf("hp = %d, want 800", m.hpLeft)
	}
	if dps, _ := rates(t, m); dps != 0 {
		t.Fatalf("dps = %d, want 0", dps)
	}
}

func TestUpdate_HPIsClamped(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), snapshotMsg(100, 50), HPMsg{HPLeft: -20})
	if m.hpLeft != 0 {
		t.Fatalf("hp = %d, want 0", m.hpLeft)
	}
	m = apply(m, HPMsg{HPLeft: 9999})
	if m.hpLeft != 100 {
		t.Fatalf("hp = %d, want 100 (hp_max)", m.hpLeft)
	}
}

func TestUpdate_RatesAverageOverWindow(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), snapshotMsg(100000, 100000))
	hp := int64(100000)
	hits := int64(0)
	// Пять секунд по 100 урона и 10 ударов.
	m = apply(m, HitsMsg{HitsTotal: hits}) // точка отсчёта
	for range rateWindow {
		hp -= 100
		hits += 10
		m = apply(m, HPMsg{HPLeft: hp}, HitsMsg{HitsTotal: hits}, tickMsg)
	}
	if dps, hps := rates(t, m); dps != 100 || hps != 10 {
		t.Fatalf("rates = %d dps, %d hps, want 100, 10", dps, hps)
	}

	// Пять секунд тишины вытесняют старые значения из окна.
	for range rateWindow {
		m = apply(m, tickMsg)
	}
	if dps, hps := rates(t, m); dps != 0 || hps != 0 {
		t.Fatalf("rates after idle window = %d, %d, want 0, 0", dps, hps)
	}
}

func TestUpdate_RatesBeforeFirstTickAreZero(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), snapshotMsg(1000, 1000), HPMsg{HPLeft: 500})
	if dps, hps := rates(t, m); dps != 0 || hps != 0 {
		t.Fatalf("rates = %d, %d, want 0, 0 (no tick yet)", dps, hps)
	}
}

func TestUpdate_FirstHitsAfterSnapshotIsBaseline(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), snapshotMsg(1000, 1000), HitsMsg{HitsTotal: 5000}, tickMsg)
	if !m.hitsKnown || m.hitsTotal != 5000 {
		t.Fatalf("hits = %d known=%v, want 5000 true", m.hitsTotal, m.hitsKnown)
	}
	if _, hps := rates(t, m); hps != 0 {
		t.Fatalf("hps = %d, want 0: накопленное до подключения не скорость", hps)
	}

	m = apply(m, HitsMsg{HitsTotal: 5030}, tickMsg)
	if _, hps := rates(t, m); hps != 15 { // (0 + 30) / 2 тика
		t.Fatalf("hps = %d, want 15", hps)
	}
}

func TestUpdate_HitsDecreaseIsAcceptedButNotCounted(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), snapshotMsg(1000, 1000), HitsMsg{HitsTotal: 100}, HitsMsg{HitsTotal: 90}, tickMsg)
	if m.hitsTotal != 90 {
		t.Fatalf("hits = %d, want 90", m.hitsTotal)
	}
	if _, hps := rates(t, m); hps != 0 {
		t.Fatalf("hps = %d, want 0", hps)
	}
}

// После обрыва новый snapshot заменяет состояние целиком: разница между старым HP и новым
// уроном не считается, а число ударов снова ждёт точку отсчёта.
func TestUpdate_SnapshotAfterReconnectReplacesState(t *testing.T) {
	t.Parallel()
	m := apply(New("r"),
		snapshotMsg(1000, 900), HitsMsg{HitsTotal: 10}, HPMsg{HPLeft: 800},
		LeaderboardMsg{Entries: []Entry{{PlayerID: 1, Damage: 200}}},
		ConnMsg{State: ConnReconnecting},
	)
	if m.conn != ConnReconnecting {
		t.Fatalf("conn = %v, want reconnecting", m.conn)
	}

	m = apply(m, snapshotMsg(1000, 100), tickMsg) // за время обрыва босс почти умер
	if m.conn != ConnLive || m.hpLeft != 100 {
		t.Fatalf("conn=%v hp=%d, want live and 100", m.conn, m.hpLeft)
	}
	if dps, _ := rates(t, m); dps != 0 {
		t.Fatalf("dps = %d, want 0: скачок HP при переподключении — не урон", dps)
	}
	if m.hitsKnown {
		t.Fatal("hits baseline survived the new snapshot")
	}
	if len(m.top) != 1 {
		t.Fatalf("top = %v: до прихода нового лидерборда остаётся прежний", m.top)
	}
}

func TestUpdate_LeaderboardIsTruncatedAndCopied(t *testing.T) {
	t.Parallel()
	in := make([]Entry, 15)
	for i := range in {
		in[i] = Entry{PlayerID: int64(i + 1), Damage: int64(100 - i)}
	}
	m := apply(New("r"), snapshotMsg(1000, 1000), LeaderboardMsg{Entries: in})
	if len(m.top) != topSize {
		t.Fatalf("top has %d rows, want %d", len(m.top), topSize)
	}
	in[0].Damage = -1 // читатель стрима может переиспользовать срез
	if m.top[0].Damage != 100 {
		t.Fatalf("top shares memory with the message: %+v", m.top[0])
	}
}

func TestUpdate_FinishedDefeated(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), snapshotMsg(1000, 40))
	m, _ = update(m, FinishedMsg{Status: raidbossV1.RaidStatus_RAID_STATUS_DEFEATED, KillerPlayerID: 9})
	if !m.finished || m.killerPlayerID != 9 || m.hpLeft != 0 {
		t.Fatalf("model after defeat = %+v", m)
	}
}

func TestUpdate_FinishedExpiredKeepsHP(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), snapshotMsg(1000, 400), FinishedMsg{Status: raidbossV1.RaidStatus_RAID_STATUS_EXPIRED})
	if !m.finished || m.hpLeft != 400 {
		t.Fatalf("model after expiry = %+v", m)
	}
}

func TestUpdate_AfterFinishDataIsIgnoredAndTimerStops(t *testing.T) {
	t.Parallel()
	m := apply(New("r"), snapshotMsg(1000, 500), FinishedMsg{Status: raidbossV1.RaidStatus_RAID_STATUS_EXPIRED})

	m = apply(m, HPMsg{HPLeft: 100}, HitsMsg{HitsTotal: 77})
	if m.hpLeft != 500 || m.hitsKnown {
		t.Fatalf("finished model accepted updates: hp=%d hitsKnown=%v", m.hpLeft, m.hitsKnown)
	}

	_, cmd := update(m, tickMsg)
	if cmd != nil {
		t.Fatal("tick after finish rescheduled the timer")
	}
}

func TestUpdate_TickReschedulesTimerWhileRunning(t *testing.T) {
	t.Parallel()
	_, cmd := update(apply(New("r"), snapshotMsg(1000, 1000)), tickMsg)
	if cmd == nil {
		t.Fatal("tick did not schedule the next tick")
	}
	if New("r").Init() == nil {
		t.Fatal("Init did not start the timer")
	}
}

func TestUpdate_QuitKeys(t *testing.T) {
	t.Parallel()
	for _, key := range []tea.KeyMsg{
		{Type: tea.KeyRunes, Runes: []rune{'q'}},
		{Type: tea.KeyCtrlC},
	} {
		_, cmd := update(New("r"), key)
		if cmd == nil {
			t.Fatalf("%q did not return a command", key.String())
		}
		if _, ok := cmd().(tea.QuitMsg); !ok {
			t.Fatalf("%q returned %T, want tea.QuitMsg", key.String(), cmd())
		}
	}

	if _, cmd := update(New("r"), tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}}); cmd != nil {
		t.Fatal("unrelated key returned a command")
	}
}

func TestUpdate_ErrorMsgQuitsAndKeepsError(t *testing.T) {
	t.Parallel()
	boom := errors.New("raid not found")
	m, cmd := update(New("r"), ErrorMsg{Err: boom})
	if !errors.Is(m.Err(), boom) {
		t.Fatalf("Err() = %v, want %v", m.Err(), boom)
	}
	if cmd == nil {
		t.Fatal("ErrorMsg did not quit the program")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Fatalf("command returned %T, want tea.QuitMsg", cmd())
	}
}

func TestUpdate_DoesNotMutatePreviousModel(t *testing.T) {
	t.Parallel()
	before := apply(New("r"), snapshotMsg(1000, 1000), HPMsg{HPLeft: 900}, tickMsg)
	_ = apply(before, HPMsg{HPLeft: 500}, tickMsg, tickMsg, LeaderboardMsg{Entries: []Entry{{PlayerID: 1}}})

	if dps, _ := rates(t, before); dps != 100 {
		t.Fatalf("previous model changed: dps = %d, want 100", dps)
	}
	if before.hpLeft != 900 || len(before.top) != 0 {
		t.Fatalf("previous model changed: %+v", before)
	}
}
