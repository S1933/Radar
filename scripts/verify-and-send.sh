#!/bin/bash
# Verify radar data + generate a test briefing (persisted in Postgres).
cd /home/pi/Projects/personal-radar
export RADAR_DB_HOST=localhost RADAR_DB_PORT=5499 RADAR_DB_USER=radar RADAR_DB_PASSWORD=radar RADAR_DB_NAME=radar
echo "=== items per source (in docker postgres) ==="
docker exec deploy-postgres-1 psql -U radar -d radar -t -c "SELECT source, count(*) FROM items GROUP BY source ORDER BY 2 DESC;" 2>/dev/null || echo "psql direct failed"
echo "=== force a fresh collect + rank + briefing via local binary ==="
/home/pi/Projects/personal-radar/scripts/run-local.sh collect 2>&1 | grep -E "collected|error" | head
/home/pi/Projects/personal-radar/scripts/run-local.sh rank 2>&1 | grep -E "ranked|error" | head
echo "=== last briefing in DB ==="
docker exec deploy-postgres-1 psql -U radar -d radar -t -c "SELECT date, left(content, 120) FROM briefings ORDER BY date DESC LIMIT 1;" 2>/dev/null
