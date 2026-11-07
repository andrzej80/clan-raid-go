package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Сроки хранения записей в топиках. raid.hits должен пережить самое долгое восстановление
// консьюмера с запасом: записи, удалённые до обработки, — потерянные удары. DLQ хранится дольше,
// чтобы успеть разобрать записи и переиграть их.
const (
	hitsRetention   = 3 * 24 * time.Hour
	eventsRetention = 7 * 24 * time.Hour
	dlqRetention    = 14 * 24 * time.Hour
)

// Число партиций. Менять его у существующего топика нельзя молча: ключ raid_id переедет в другую
// партицию, и порядок ударов одного рейда на время перехода не гарантирован (см. runbook).
const (
	hitsPartitions    = 12
	eventsPartitions  = 6
	rewardsPartitions = 6
	dlqPartitions     = 1
)

// TopicSpec — желаемое состояние топика.
type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	// Configs — управляемые настройки топика; остальные остаются по умолчанию брокера.
	Configs map[string]string
}

// TopicsConfig — имена топиков и фактор репликации кластера.
type TopicsConfig struct {
	HitsTopic         string
	HitsDLQTopic      string
	EventsTopic       string
	EventsDLQTopic    string
	RewardsTopic      string
	ReplicationFactor int16
}

// MinInSyncReplicas — сколько реплик должны подтвердить запись при acks=all. Для RF=3 это 2:
// кластер переживает потерю одного брокера без потери подтверждённых записей и без остановки
// записи. Для RF меньше 3 — 1: иначе потеря любого брокера останавливала бы запись.
func MinInSyncReplicas(rf int16) int16 {
	if rf >= 3 {
		return 2
	}
	return 1
}

// Topics возвращает топики сервиса. Это единственное место, где задаются партиции, сроки хранения
// и min.insync.replicas; raidctl topics сверяет с ним кластер.
func Topics(cfg TopicsConfig) []TopicSpec {
	rf := max(cfg.ReplicationFactor, 1)
	spec := func(name string, partitions int32, retention time.Duration) TopicSpec {
		return TopicSpec{
			Name:              name,
			Partitions:        partitions,
			ReplicationFactor: rf,
			Configs: map[string]string{
				"cleanup.policy":      "delete",
				"retention.ms":        strconv.FormatInt(retention.Milliseconds(), 10),
				"min.insync.replicas": strconv.Itoa(int(MinInSyncReplicas(rf))),
			},
		}
	}
	return []TopicSpec{
		spec(cfg.HitsTopic, hitsPartitions, hitsRetention),
		spec(cfg.HitsDLQTopic, dlqPartitions, dlqRetention),
		spec(cfg.EventsTopic, eventsPartitions, eventsRetention),
		spec(cfg.EventsDLQTopic, dlqPartitions, dlqRetention),
		spec(cfg.RewardsTopic, rewardsPartitions, eventsRetention),
	}
}

// DriftKind — вид расхождения кластера с TopicSpec.
type DriftKind string

const (
	DriftMissing     DriftKind = "missing"     // топика нет
	DriftPartitions  DriftKind = "partitions"  // другое число партиций
	DriftReplication DriftKind = "replication" // другой фактор репликации
	DriftConfig      DriftKind = "config"      // другое значение настройки
)

// Drift — одно расхождение. Fixable — может ли его исправить Apply. Число партиций и фактор
// репликации Apply не меняет: это операции с последствиями (порядок по ключу, перенос данных),
// их делают по runbook.
type Drift struct {
	Topic   string
	Kind    DriftKind
	Key     string // имя настройки для DriftConfig
	Want    string
	Got     string
	Fixable bool
}

func (d Drift) String() string {
	switch d.Kind {
	case DriftMissing:
		return d.Topic + ": missing"
	case DriftConfig:
		return fmt.Sprintf("%s: config %s = %q, want %q", d.Topic, d.Key, d.Got, d.Want)
	case DriftPartitions, DriftReplication:
	}
	return fmt.Sprintf("%s: %s = %s, want %s", d.Topic, d.Kind, d.Got, d.Want)
}

// TopicAdmin сверяет и приводит топики кластера к TopicSpec.
type TopicAdmin struct {
	cl  *kgo.Client
	adm *kadm.Client
}

// NewTopicAdmin оборачивает клиент Kafka. Закрывать клиент — забота вызывающего.
func NewTopicAdmin(cl *kgo.Client) *TopicAdmin {
	return &TopicAdmin{cl: cl, adm: kadm.NewClient(cl)}
}

// Diff возвращает расхождения кластера со specs; пустой список — кластер совпадает.
func (a *TopicAdmin) Diff(ctx context.Context, specs []TopicSpec) ([]Drift, error) {
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.Name
	}
	details, err := a.adm.ListTopics(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}

	var (
		drifts   []Drift
		existing []TopicSpec
	)
	for _, s := range specs {
		d, ok := details[s.Name]
		switch {
		case !ok, errors.Is(d.Err, kerr.UnknownTopicOrPartition):
			drifts = append(drifts, Drift{Topic: s.Name, Kind: DriftMissing, Fixable: true})
			continue
		case d.Err != nil:
			return nil, fmt.Errorf("describe topic %s: %w", s.Name, d.Err)
		}
		existing = append(existing, s)
		drifts = append(drifts, layoutDrifts(s, d)...)
	}

	configDrifts, err := a.configDrifts(ctx, existing)
	if err != nil {
		return nil, err
	}
	return append(drifts, configDrifts...), nil
}

// layoutDrifts сравнивает число партиций и фактор репликации (по каждой партиции).
func layoutDrifts(s TopicSpec, d kadm.TopicDetail) []Drift {
	var out []Drift
	if got := int32(len(d.Partitions)); got != s.Partitions { //nolint:gosec // партиций — единицы и десятки
		out = append(out, Drift{
			Topic: s.Name, Kind: DriftPartitions,
			Want: strconv.Itoa(int(s.Partitions)), Got: strconv.Itoa(int(got)),
		})
	}
	var factors []int
	for _, p := range d.Partitions {
		if n := len(p.Replicas); n != int(s.ReplicationFactor) && !slices.Contains(factors, n) {
			factors = append(factors, n)
		}
	}
	if len(factors) > 0 {
		slices.Sort(factors)
		got := make([]string, len(factors))
		for i, f := range factors {
			got[i] = strconv.Itoa(f)
		}
		out = append(out, Drift{
			Topic: s.Name, Kind: DriftReplication,
			Want: strconv.Itoa(int(s.ReplicationFactor)), Got: fmt.Sprint(got),
		})
	}
	return out
}

func (a *TopicAdmin) configDrifts(ctx context.Context, specs []TopicSpec) ([]Drift, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.Name
	}
	described, err := a.adm.DescribeTopicConfigs(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("describe topic configs: %w", err)
	}

	var out []Drift
	for _, s := range specs {
		rc, err := described.On(s.Name, nil)
		if err != nil {
			return nil, fmt.Errorf("describe configs of %s: %w", s.Name, err)
		}
		if rc.Err != nil {
			return nil, fmt.Errorf("describe configs of %s: %w", s.Name, rc.Err)
		}
		got := make(map[string]string, len(rc.Configs))
		for _, c := range rc.Configs {
			if c.Value != nil {
				got[c.Key] = *c.Value
			}
		}
		for _, key := range sortedKeys(s.Configs) {
			if want := s.Configs[key]; got[key] != want {
				out = append(out, Drift{
					Topic: s.Name, Kind: DriftConfig, Key: key, Want: want, Got: got[key], Fixable: true,
				})
			}
		}
	}
	return out, nil
}

// Apply создаёт недостающие топики и выставляет расходящиеся настройки. Возвращает расхождения,
// которые остались (партиции и фактор репликации): их Apply не трогает.
func (a *TopicAdmin) Apply(ctx context.Context, specs []TopicSpec) (applied, remaining []Drift, err error) {
	drifts, err := a.Diff(ctx, specs)
	if err != nil {
		return nil, nil, err
	}
	byName := make(map[string]TopicSpec, len(specs))
	for _, s := range specs {
		byName[s.Name] = s
	}

	alters := map[string][]kadm.AlterConfig{}
	for _, d := range drifts {
		switch d.Kind {
		case DriftMissing:
			if err := a.create(ctx, byName[d.Topic]); err != nil {
				return applied, nil, err
			}
			applied = append(applied, d)
		case DriftConfig:
			alters[d.Topic] = append(alters[d.Topic], kadm.AlterConfig{Op: kadm.SetConfig, Name: d.Key, Value: &d.Want})
			applied = append(applied, d)
		case DriftPartitions, DriftReplication:
			remaining = append(remaining, d)
		}
	}

	// kgo кэширует метаданные топиков: без сброса Diff в том же процессе видел бы новые топики
	// отсутствующими.
	var created []string
	for _, d := range applied {
		if d.Kind == DriftMissing {
			created = append(created, d.Topic)
		}
	}
	a.cl.PurgeTopicsFromClient(created...)

	for _, topic := range sortedKeys(alters) {
		resp, err := a.adm.AlterTopicConfigs(ctx, alters[topic], topic)
		if err != nil {
			return applied, remaining, fmt.Errorf("alter configs of %s: %w", topic, err)
		}
		for _, r := range resp {
			if r.Err != nil {
				return applied, remaining, fmt.Errorf("alter configs of %s: %w (%s)", topic, r.Err, r.ErrMessage)
			}
		}
	}
	return applied, remaining, nil
}

func (a *TopicAdmin) create(ctx context.Context, s TopicSpec) error {
	configs := make(map[string]*string, len(s.Configs))
	for k, v := range s.Configs {
		configs[k] = &v
	}
	// CreateTopic возвращает и ошибку запроса, и код ответа брокера. Топик, созданный между Diff и
	// созданием (параллельный raidctl), — не ошибка: настройки сверит следующий запуск.
	resp, err := a.adm.CreateTopic(ctx, s.Partitions, s.ReplicationFactor, configs, s.Name)
	if errors.Is(err, kerr.TopicAlreadyExists) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("create topic %s: %w (%s)", s.Name, err, resp.ErrMessage)
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
