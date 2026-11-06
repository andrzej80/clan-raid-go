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
	"sync"
	"syscall"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raidbossV1 "github.com/andrzej80/clan-raid-go/gen/raidboss/v1"
	"github.com/andrzej80/clan-raid-go/internal/raidwatch"
)

func main() {
	os.Exit(realMain())
}

// stdinIsTerminal сообщает, подключён ли stdin к терминалу. В Git Bash (mintty) нативная программа
// получает stdin как pipe: bubbletea не может читать клавиши, и q не работает.
func stdinIsTerminal() bool {
	info, err := os.Stdin.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

// realMain отделён от main, чтобы defer stop() отработал до os.Exit.
func realMain() int {
	fs := flag.NewFlagSet("raidwatch", flag.ContinueOnError)
	raidID := fs.String("raid", "", "id рейда (uuid)")
	addr := fs.String("addr", "localhost:9090", "адрес gRPC-сервера api")
	player := fs.Int64("player", 1, "id игрока для заголовка x-player-id (сервер требует его и для WatchRaid)")
	force := fs.Bool("force", false, "запускаться, даже если stdin не терминал (клавиши тогда не работают)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return 2
	}
	if _, err := uuid.Parse(*raidID); err != nil {
		fmt.Fprintln(os.Stderr, "raidwatch: нужен --raid <uuid>")
		return 2
	}

	if !*force && !stdinIsTerminal() {
		fmt.Fprintln(os.Stderr, "raidwatch: stdin не терминал, клавиши (q) до программы не дойдут.\n"+
			"В Git Bash (mintty) запустите через winpty (winpty raidwatch --raid ...) или из Windows Terminal/PowerShell.\n"+
			"--force запускает всё равно, выйти тогда можно только по Ctrl+C.")
		return 2
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "raidwatch:", err)
		return 1
	}
	defer conn.Close() //nolint:errcheck // выход из программы

	// Ctrl+C приходит и как сигнал, и как клавиша в raw-режиме терминала (её ловит сама программа).
	// В обоих случаях отмена ctx закрывает стрим.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Полноэкранный режим: кадр выше окна в обычном режиме оставляет копии строк в прокрутке.
	p := tea.NewProgram(raidwatch.New(*raidID), tea.WithContext(ctx), tea.WithAltScreen())
	var wg sync.WaitGroup
	wg.Go(func() {
		raidwatch.Watch(ctx, raidbossV1.NewRaidServiceClient(conn),
			raidwatch.WatchConfig{RaidID: *raidID, PlayerID: *player}, p.Send)
	})

	final, err := p.Run()
	cancel() // программа закончилась: останавливаем читателя и ждём его выхода
	wg.Wait()
	if err != nil {
		fmt.Fprintln(os.Stderr, "raidwatch:", err)
		return 1
	}
	if m, ok := final.(raidwatch.Model); ok && m.Err() != nil {
		fmt.Fprintln(os.Stderr, "raidwatch:", m.Err())
		return 1
	}
	return 0
}
