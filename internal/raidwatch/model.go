// Package raidwatch — терминальный клиент для WatchRaid на bubbletea.
//
// Состояние экрана лежит в Model, все изменения идут через Update, отрисовка — в View.
// Сетевой код в Model не попадает: он присылает в программу сообщения tea.Msg.
// Update не читает часы и не ходит в сеть, поэтому проверяется обычными тестами.
package raidwatch

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

const (
	topSize      = 10          // сколько строк лидерборда показываем
	tickInterval = time.Second // шаг окна урона в секунду
	rateWindow   = 5           // сколько последних тиков усредняем
)

// Model — состояние экрана raidwatch.
//
// Update возвращает копию модели, поэтому в ней нет слайсов, которые мутируются на месте:
// окно урона — массив, топ каждый раз заменяется новым слайсом.
type Model struct {
	raidID string
	conn   ConnState
	width  int // размеры терминала; 0, пока не пришёл WindowSizeMsg
	height int

	haveSnapshot bool
	bossName     string
	hpMax        int64
	hpLeft       int64
	hitsTotal    int64
	hitsKnown    bool // hitsTotal получен от сервера после последнего snapshot
	status       raidbossV1.RaidStatus
	top          []Entry

	// Скорость считается по разнице абсолютных значений (убыль HP и прирост числа ударов), а не по
	// числу сообщений: сервер шлёт обновления не чаще 4 раз в секунду на рейд и схлопывает лишние.
	// pending копит разницу до очередного тика, затем она уходит в кольцо window.
	pending sample
	window  [rateWindow]sample
	filled  int // сколько ячеек window заполнено
	next    int // куда писать следующий тик

	err error // фатальная ошибка стрима

	finished       bool
	finalStatus    raidbossV1.RaidStatus
	killerPlayerID int64
}

// New создаёт модель для рейда raidID.
func New(raidID string) Model {
	return Model{raidID: raidID, conn: ConnConnecting}
}

// Err возвращает фатальную ошибку стрима, из-за которой программа завершилась.
func (m Model) Err() error { return m.err }

// Init запускает таймер урона в секунду. Сетевые горутины стартуют снаружи и шлют сообщения
// через Program.Send.
func (m Model) Init() tea.Cmd { return tick() }

func tick() tea.Cmd {
	return tea.Tick(tickInterval, func(t time.Time) tea.Msg { return TickMsg(t) })
}

// Update применяет сообщение к состоянию.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if s := msg.String(); s == "q" || s == "ctrl+c" {
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
	case SnapshotMsg:
		m = m.applySnapshot(msg)
	case HPMsg:
		m = m.applyHP(msg.HPLeft)
	case HitsMsg:
		m = m.applyHits(msg.HitsTotal)
	case LeaderboardMsg:
		m.top = copyTop(msg.Entries)
	case FinishedMsg:
		m.finished = true
		m.finalStatus = msg.Status
		m.killerPlayerID = msg.KillerPlayerID
		if msg.Status == raidbossV1.RaidStatus_RAID_STATUS_DEFEATED {
			// Hub при finished выбрасывает ещё не отправленные обновления, так что последний hp_left
			// мог не дойти; у побеждённого босса HP в любом случае 0.
			m.hpLeft = 0
		}
	case ConnMsg:
		m.conn = msg.State
	case ErrorMsg:
		m.err = msg.Err
		return m, tea.Quit
	case TickMsg:
		m = m.pushTick()
		if m.finished {
			return m, nil // таймер больше не нужен
		}
		return m, tick()
	}
	return m, nil
}

// applySnapshot целиком заменяет состояние рейда. Скачок HP между старым значением и снимком
// (например, после обрыва) уроном не считается: pending обнуляется. Топ не трогаем: после
// snapshot сервер сам присылает свежий лидерборд, а до этого прошлый лучше пустого экрана.
func (m Model) applySnapshot(s SnapshotMsg) Model {
	m.haveSnapshot = true
	m.conn = ConnLive
	m.bossName = s.BossName
	m.hpMax = s.HPMax
	m.hpLeft = clampHP(s.HPLeft, s.HPMax)
	m.hitsTotal, m.hitsKnown = 0, false
	m.status = s.Status
	m.pending = sample{}
	return m
}

// applyHP принимает абсолютное значение hp_left. Убыль идёт в счётчик урона, рост (поздний
// пакет, восстановление HP) просто принимается как есть.
func (m Model) applyHP(hp int64) Model {
	if !m.haveSnapshot || m.finished {
		return m
	}
	hp = clampHP(hp, m.hpMax)
	if hp < m.hpLeft {
		m.pending.damage += m.hpLeft - hp
	}
	m.hpLeft = hp
	return m
}

// applyHits принимает абсолютное число ударов; прирост идёт в счётчик скорости, кроме первого
// значения после snapshot (иначе после переподключения весь накопленный счёт стал бы «скачком»).
func (m Model) applyHits(total int64) Model {
	if !m.haveSnapshot || m.finished {
		return m
	}
	total = max(total, 0)
	if m.hitsKnown && total > m.hitsTotal {
		m.pending.hits += total - m.hitsTotal
	}
	m.hitsTotal, m.hitsKnown = total, true
	return m
}

// pushTick закрывает текущую секунду: пишет накопленную убыль HP в кольцо и обнуляет счётчик.
func (m Model) pushTick() Model {
	m.window[m.next] = m.pending
	m.next = (m.next + 1) % rateWindow
	m.filled = min(m.filled+1, rateWindow)
	m.pending = sample{}
	return m
}

// sample — что изменилось за один тик.
type sample struct {
	damage int64 // на сколько убыло HP
	hits   int64 // сколько ударов принято
}

// Rates возвращает средние за последние тики урон и число ударов в секунду
// (нули, пока не было ни одного тика).
func (m Model) Rates() (dps, hps int64) {
	if m.filled == 0 {
		return 0, 0
	}
	var sum sample
	for i := range m.filled {
		sum.damage += m.window[i].damage
		sum.hits += m.window[i].hits
	}
	return sum.damage / int64(m.filled), sum.hits / int64(m.filled)
}

func clampHP(hp, hpMax int64) int64 {
	hp = max(hp, 0)
	if hpMax > 0 {
		hp = min(hp, hpMax)
	}
	return hp
}

// copyTop копирует первые topSize строк: сообщение читателя не должно делить память с моделью.
func copyTop(in []Entry) []Entry {
	return append([]Entry(nil), in[:min(len(in), topSize)]...)
}
