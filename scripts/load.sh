#!/usr/bin/env bash
# Generate continuous load so the Grafana dashboard shows live rates.
#
#   RATE=20 DURATION=300 ./scripts/load.sh
#
# RATE is jobs per second (approximate), DURATION is seconds; 0 runs until
# interrupted.
set -euo pipefail

API="${GOQUEUE_API:-http://localhost:8080/api/v1}"
RATE="${RATE:-10}"
DURATION="${DURATION:-60}"

TASKS=("email.send" "report.generate" "demo.flaky")
QUEUES=("critical" "default" "low")
PRIORITIES=("critical" "normal" "low")

interval=$(awk -v r="$RATE" 'BEGIN { printf "%.4f", 1/r }')
started=$(date +%s)
sent=0

echo "==> sending ~${RATE} jobs/s to $API for ${DURATION}s (Ctrl-C to stop)"
trap 'echo; echo "==> sent $sent jobs"; exit 0' INT TERM

while true; do
  if [ "$DURATION" -gt 0 ] && [ $(( $(date +%s) - started )) -ge "$DURATION" ]; then
    break
  fi

  i=$(( RANDOM % 3 ))
  curl -sS -o /dev/null -X POST "$API/jobs" \
    -H 'Content-Type: application/json' \
    -d "{\"task_type\":\"${TASKS[$i]}\",\"queue\":\"${QUEUES[$i]}\",\"priority\":\"${PRIORITIES[$i]}\",\"max_retries\":2,\"retry_base_delay_seconds\":2,\"payload\":{\"to\":\"load@example.com\",\"subject\":\"load test\",\"n\":$sent}}" \
    || true

  sent=$(( sent + 1 ))
  sleep "$interval"
done

echo "==> sent $sent jobs"
