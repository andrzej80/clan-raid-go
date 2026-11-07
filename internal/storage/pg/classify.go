package pg

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrzej80/clan-raid-go/internal/domain"
)

// ClassifyError решает, поможет ли повтор операции с Postgres (см. domain.ErrorClass).
//
// Постоянные — только классы SQLSTATE, которые говорят о самих данных: 22 (data exception —
// переполнение, неверный формат) и 23 (нарушение ограничения). Ошибки схемы и кода (42 — нет
// таблицы или колонки, XX — внутренняя ошибка) намеренно неизвестные, а не постоянные: после
// неудачного релиза или миграции они бьют по каждой записи, и считать их постоянными значило бы
// унести в DLQ весь поток.
func ClassifyError(err error) domain.ErrorClass {
	if err == nil {
		return domain.ErrorUnknown
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || pgconn.Timeout(err) {
		return domain.ErrorTransient
	}

	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return classifySQLState(pgErr.Code)
	}

	_, connectErr := errors.AsType[*pgconn.ConnectError](err)
	_, netErr := errors.AsType[net.Error](err)
	if connectErr || netErr || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return domain.ErrorTransient
	}
	return domain.ErrorUnknown
}

func classifySQLState(code string) domain.ErrorClass {
	if len(code) < 2 {
		return domain.ErrorUnknown
	}
	switch code[:2] {
	case "08", // connection exception
		"40", // transaction rollback: serialization_failure, deadlock_detected
		"53", // insufficient resources: нет памяти, места, соединений
		"55", // object not in prerequisite state: lock_not_available
		"57", // operator intervention: query_canceled, admin_shutdown, cannot_connect_now
		"58": // system error: ошибка ввода-вывода
		return domain.ErrorTransient
	case "22", // data exception
		"23": // integrity constraint violation
		return domain.ErrorPermanent
	}
	return domain.ErrorUnknown
}
