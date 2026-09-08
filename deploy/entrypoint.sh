#!/bin/sh
# The radar container runs a single process: collect -> rank -> briefings
# in Postgres. There is no web dashboard and no Telegram bot anymore; the
# chat agent reads the database directly.
set -eu

CONFIG="${RADAR_CONFIG:-config/radar.yaml}"

echo "[entrypoint] starting radar run (scheduler: collect + briefing slots)"
exec radar run -config "$CONFIG"
