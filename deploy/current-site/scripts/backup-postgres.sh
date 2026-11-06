#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="${APP_DIR:-$(cd -- "$SCRIPT_DIR/.." && pwd)}"
COMPOSE_FILE="$APP_DIR/compose.yaml"
ENV_FILE="$APP_DIR/.env"
BACKUP_DIR="$APP_DIR/backups/postgres"
MARKER="$APP_DIR/backups/new-api.last-success"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_FILE="$BACKUP_DIR/new-api-$STAMP.dump"

[[ -f "$COMPOSE_FILE" && -f "$ENV_FILE" ]] || {
  echo "Deployment files not found under $APP_DIR" >&2
  exit 1
}

mkdir -p "$BACKUP_DIR"
chmod 700 "$APP_DIR/backups" "$BACKUP_DIR"
TEMP_FILE="$(mktemp "$BACKUP_DIR/.new-api-$STAMP.XXXXXX")"
trap 'rm -f "$TEMP_FILE"' EXIT

docker compose --project-directory "$APP_DIR" --env-file "$ENV_FILE" -f "$COMPOSE_FILE" \
  exec -T postgres sh -ec 'pg_dump --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --format=custom' \
  > "$TEMP_FILE"

[[ -s "$TEMP_FILE" ]] || { echo "Database dump is empty" >&2; exit 1; }
docker compose --project-directory "$APP_DIR" --env-file "$ENV_FILE" -f "$COMPOSE_FILE" \
  exec -T postgres pg_restore --list < "$TEMP_FILE" > /dev/null

chmod 600 "$TEMP_FILE"
mv -- "$TEMP_FILE" "$BACKUP_FILE"
date -u +%Y-%m-%dT%H:%M:%SZ > "$MARKER.tmp"
chmod 600 "$MARKER.tmp"
mv -- "$MARKER.tmp" "$MARKER"

find "$BACKUP_DIR" -type f -name 'new-api-*.dump' -mtime +14 -delete
echo "Created and verified $BACKUP_FILE"
