package raidwatch

import (
	"sync"

	tea "github.com/charmbracelet/bubbletea"
)

// mailbox — очередь между читателем стрима и программой bubbletea.
//
// put не блокируется никогда, поэтому читатель стрима не зависит от скорости UI. Очередь при этом не
// растёт: hp_left, число ударов и лидерборд абсолютные, и устаревшее значение заменяется новым прямо
// в очереди. Snapshot, финал и смена состояния соединения не теряются и идут по порядку.
type mailbox struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []tea.Msg
	closed bool
}

func newMailbox() *mailbox {
	b := &mailbox{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// put кладёт сообщение в очередь.
func (b *mailbox) put(msg tea.Msg) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	switch msg.(type) {
	case SnapshotMsg:
		// Snapshot начинает новый стрим: значения из очереди относятся к старому и уже не нужны.
		b.queue = dropCoalescible(b.queue)
	case HPMsg, HitsMsg, LeaderboardMsg:
		if i := indexOfSameType(b.queue, msg); i >= 0 {
			b.queue[i] = msg
			return // длина очереди не изменилась, ждущий next уже разбужен
		}
	}
	b.queue = append(b.queue, msg)
	b.cond.Signal()
}

// next ждёт сообщение. После close отдаёт то, что осталось в очереди, и затем false.
func (b *mailbox) next() (tea.Msg, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.queue) == 0 && !b.closed {
		b.cond.Wait()
	}
	if len(b.queue) == 0 {
		return nil, false
	}
	msg := b.queue[0]
	b.queue[0] = nil
	b.queue = b.queue[1:]
	return msg, true
}

// close завершает очередь: новые сообщения не принимаются, next дочитывает остаток.
func (b *mailbox) close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.cond.Broadcast()
}

func isCoalescible(msg tea.Msg) bool {
	switch msg.(type) {
	case HPMsg, HitsMsg, LeaderboardMsg:
		return true
	}
	return false
}

func dropCoalescible(queue []tea.Msg) []tea.Msg {
	kept := queue[:0]
	for _, m := range queue {
		if !isCoalescible(m) {
			kept = append(kept, m)
		}
	}
	clear(queue[len(kept):])
	return kept
}

// indexOfSameType ищет в очереди сообщение того же типа, что и msg.
func indexOfSameType(queue []tea.Msg, msg tea.Msg) int {
	for i, m := range queue {
		switch msg.(type) {
		case HPMsg:
			if _, ok := m.(HPMsg); ok {
				return i
			}
		case HitsMsg:
			if _, ok := m.(HitsMsg); ok {
				return i
			}
		case LeaderboardMsg:
			if _, ok := m.(LeaderboardMsg); ok {
				return i
			}
		}
	}
	return -1
}
