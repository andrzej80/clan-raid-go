package raidwatch

import (
	"time"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
)

// Сообщения, которые читатель стрима и таймер шлют в программу bubbletea.
// Здесь свои типы, а не protobuf: Update тогда не зависит от формата сообщений на проводе,
// а тесты строят сообщения без генерированного кода.

// ConnState — состояние соединения со стримом.
type ConnState int

const (
	ConnConnecting   ConnState = iota // первое подключение
	ConnLive                          // стрим работает
	ConnReconnecting                  // обрыв, ждём повтора
)

// Entry — строка лидерборда.
type Entry struct {
	PlayerID int64
	Damage   int64
}

// SnapshotMsg — первое сообщение каждого стрима. Целиком заменяет состояние рейда:
// так клиент после переподключения возвращается к актуальной картине.
type SnapshotMsg struct {
	BossName string
	HPMax    int64
	HPLeft   int64
	Status   raidbossV1.RaidStatus
}

// HPMsg — новое значение hp_left. Оно абсолютное, а не приращение, поэтому потерянное
// или схлопнутое обновление ничего не портит.
type HPMsg struct {
	HPLeft int64
}

// HitsMsg — новое значение числа принятых ударов. Как и hp_left, оно абсолютное: если сервер
// схлопнул несколько обновлений, в сумме ничего не теряется. Первое значение после snapshot
// считается точкой отсчёта, а не приростом: в snapshot числа ударов нет.
type HitsMsg struct {
	HitsTotal int64
}

// LeaderboardMsg — актуальный топ игроков, уже отсортированный сервером.
type LeaderboardMsg struct {
	Entries []Entry
}

// FinishedMsg — рейд закончился (последнее сообщение стрима).
type FinishedMsg struct {
	Status         raidbossV1.RaidStatus
	KillerPlayerID int64
}

// ConnMsg сообщает об изменении состояния соединения.
type ConnMsg struct {
	State ConnState
}

// ErrorMsg — ошибка, после которой стрим не переподключается (например, рейда нет).
type ErrorMsg struct {
	Err error
}

// TickMsg приходит раз в tickInterval: сдвигает окно для расчёта урона в секунду.
type TickMsg time.Time
