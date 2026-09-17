# `POSTGRES_PASSWORD=airflow`, and it's hardcoded a second time — changing `.env` alone won't fix it

**Status:** open — confirmed live, 2026-09-17. Airflow's own admin password was rotated; Postgres's was not, and can't be via `.env` edit alone.

## What

Two separate problems, confirmed by direct inspection:

1. **The password itself is weak.** `airflow/.env`:
   ```
   POSTGRES_PASSWORD=airflow
   ```
   Username `airflow`, password `airflow`, on the database backing Airflow's
   metadata (DAG state, connections, variables — some of which hold other
   secrets).

2. **It's hardcoded a second time, in `docker-compose.yml`, bypassing `.env`
   entirely for the actual connection:**
   ```yaml
   # docker-compose.yml:64
   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
   # docker-compose.yml:68
   AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
   ```
   Both connection strings have `airflow:airflow` typed literally, not
   `${POSTGRES_USER}:${POSTGRES_PASSWORD}`. The `postgres:` service itself
   *does* read `POSTGRES_PASSWORD` from `.env` correctly
   (`docker-compose.yml:127`) — so changing `.env`'s `POSTGRES_PASSWORD` and
   restarting would actually **break** the stack (Postgres comes up with the
   new password, but the webserver/scheduler/worker still try to connect
   with the hardcoded `airflow:airflow`) rather than fix it, unless both
   compose lines are updated in the same change.

This was noticed because `_AIRFLOW_WWW_USER_PASSWORD` (the Airflow UI admin
login) *was* rotated to something non-default, while `POSTGRES_PASSWORD` was
not — likely because rotating the UI password is a one-line `.env` edit with
an obvious effect, while the Postgres password is wired through in a way
that isn't obvious from `.env` alone.

## Why it matters

This is the database backing Airflow's own metadata store — not just KG
data. A weak, default-matching-username credential here is a much softer
target than anything else in the stack, and it's one that a `.env`-only
audit would miss, because `.env` alone doesn't show that the real connection
string ignores it.

## TODO

- [ ] Pick a real password for `POSTGRES_PASSWORD` in `.env` (not `airflow`,
      not matching `POSTGRES_USER`).
- [ ] **In the same change**, update both hardcoded connection strings in
      `docker-compose.yml` (lines ~64 and ~68) to interpolate
      `${POSTGRES_USER}:${POSTGRES_PASSWORD}` instead of the literal
      `airflow:airflow`, so `.env` becomes the actual single source of truth
      it already is for every other credential in this file.
- [ ] After the change: `docker compose down && docker compose up -d`, then
      confirm the webserver/scheduler/worker/dag-processor all report
      healthy and can reach Postgres (`docker compose ps`, check for
      connection-refused loops in `docker compose logs postgres airflow-webserver`).
- [ ] Grep the rest of `docker-compose.yml` for any other place a credential
      is typed literally instead of interpolated from `.env` — this pattern
      (env var exists and is read by one service, but hardcoded elsewhere)
      is exactly the kind of thing that hides from a quick `.env` review.
