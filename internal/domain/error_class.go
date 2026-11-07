package domain

// ErrorClass — поможет ли повтор операции, которая завершилась ошибкой. Его определяет хранилище
// (pg.ClassifyError), а использует консьюмер: решает, повторять пачку или убирать запись в DLQ.
type ErrorClass int

const (
	// ErrorUnknown — неизвестно. Повторять в пределах бюджета, затем искать виновную запись.
	ErrorUnknown ErrorClass = iota
	// ErrorTransient — временный сбой: сеть, таймаут, deadlock, конфликт сериализации, остановка
	// сервера. Повторять бесконечно: отправка в DLQ при лежащей БД унесла бы туда весь поток.
	ErrorTransient
	// ErrorPermanent — запись нельзя применить, сколько ни повторяй: данные нарушают ограничения
	// или формат. Найти виновную запись и убрать в DLQ.
	ErrorPermanent
)

func (c ErrorClass) String() string {
	switch c {
	case ErrorTransient:
		return "transient"
	case ErrorPermanent:
		return "permanent"
	case ErrorUnknown:
		return "unknown"
	}
	return "unknown"
}
