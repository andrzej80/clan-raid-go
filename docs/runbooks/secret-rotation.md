# Ротация секретов

В стенде секреты лежат в `deploy/.env`: пароль Postgres; Redis и Kafka без аутентификации. В продакшене секреты должны храниться в Secret или внешнем хранилище (этап 11, B8). Порядок ротации от этого не меняется.

## Пароль Postgres

Старый пароль нельзя отключить, пока все сервисы не перешли на новый. Поэтому нужна вторая роль, иначе при перезапуске сервисы по очереди теряли бы доступ.

1. Создать вторую роль с теми же правами и новым паролем:
   ```bash
   psql_q "CREATE ROLE raidboss_v2 LOGIN PASSWORD '<новый>' IN ROLE <текущая_роль>"
   ```
2. Переключить `DATABASE_URL` на новую роль и перезапустить сервисы по одному, проверяя `/readyz` после каждого:
   ```bash
   $DC up -d --force-recreate --no-deps api
   docker exec api wget -qO- http://127.0.0.1:8081/readyz
   ```
   Затем так же `hits-consumer`, `outbox-relay`, `rewards-worker` и `scheduler`. Перезапуск безопасен: незакоммиченные пачки применятся повторно без дублей.
3. Убедиться, что под старой ролью больше нет соединений, и отключить ей вход:
   ```bash
   psql_q "SELECT usename, count(*) FROM pg_stat_activity GROUP BY 1"
   psql_q "ALTER ROLE <старая_роль> NOLOGIN"
   ```

## Проверка

Все сервисы отвечают `/readyz` 200, алертов `ServiceNotReporting` нет, `go run ./cmd/raidctl verify` проходит.
