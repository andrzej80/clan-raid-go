// raidwatch — терминальный клиент: показывает ход рейда по стриму WatchRaid.
//
//	raidwatch --raid <uuid> [--addr localhost:9090] [--player 1]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/google/uuid"

	"github.com/andrzej80/clan-raid-go/internal/raidwatch"
)

func main() {
	os.Exit(realMain())
}

// realMain отделён от main, чтобы defer stop() отработал до os.Exit.
func realMain() int {
	fs := flag.NewFlagSet("raidwatch", flag.ContinueOnError)
	raidID := fs.String("raid", "", "id рейда (uuid)")
	addr := fs.String("addr", "localhost:9090", "адрес gRPC-сервера api")
	player := fs.Int64("player", 1, "id игрока для заголовка x-player-id (сервер требует его и для WatchRaid)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return 2
	}
	if _, err := uuid.Parse(*raidID); err != nil {
		fmt.Fprintln(os.Stderr, "raidwatch: нужен --raid <uuid>")
		return 2
	}
	_ = addr
	_ = player

	// Ctrl+C приходит и как сигнал, и как клавиша в raw-режиме терминала: отмена ctx закроет стрим в части 3.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	p := tea.NewProgram(raidwatch.New(*raidID), tea.WithContext(ctx))
	if _, err := p.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "raidwatch:", err)
		return 1
	}
	return 0
}
