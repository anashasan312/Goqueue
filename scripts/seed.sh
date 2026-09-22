#!/usr/bin/env bash
# Enqueue a spread of demo jobs so the dashboard and Grafana have something to
# show: immediate work, a scheduled job, a deliberately flaky one that exercises
# the retry and dead-letter paths, and a job that outlives its timeout.
set -euo pipefail

API="${GOQUEUE_API:-http://localhost:8080/api/v1}"

enqueue() {
  curl -sS -X POST "$API/jobs" \
    -H 'Content-Type: application/json' \
    -d "$1" | sed 's/^/  /'
  echo
}

echo "==> enqueueing demo jobs against $API"

echo "-- email.send on the critical queue"
enqueue '{
  "task_type": "email.send",
  "queue": "critical",
  "priority": "critical",
  "payload": {"to":"anas@example.com","subject":"Welcome","body":"Hello!"}
}'

echo "-- report.generate on the default queue"
enqueue '{
  "task_type": "report.generate",
  "queue": "default",
  "priority": "normal",
  "payload": {"report_id":"q3-revenue"}
}'

echo "-- demo.flaky with a short backoff, to exercise retries and the DLQ"
enqueue '{
  "task_type": "demo.flaky",
  "queue": "default",
  "max_retries": 3,
  "retry_base_delay_seconds": 2,
  "backoff_strategy": "exponential",
  "payload": {"note":"fails roughly half the time"}
}'

echo "-- demo.slow with a 1s timeout, to show a timeout being retried"
enqueue '{
  "task_type": "demo.slow",
  "queue": "low",
  "priority": "low",
  "timeout_seconds": 1,
  "max_retries": 2,
  "payload": {}
}'

echo "-- scheduled: email.send in 30 seconds"
enqueue '{
  "task_type": "email.send",
  "queue": "default",
  "delay_seconds": 30,
  "payload": {"to":"later@example.com","subject":"Scheduled","body":"Sent later"}
}'

echo "-- idempotent: the same key twice returns the same job"
enqueue '{
  "task_type": "email.send",
  "idempotency_key": "welcome-user-42",
  "payload": {"to":"user42@example.com","subject":"Once only","body":"Deduplicated"}
}'
enqueue '{
  "task_type": "email.send",
  "idempotency_key": "welcome-user-42",
  "payload": {"to":"user42@example.com","subject":"Once only","body":"Deduplicated"}
}'

echo "==> done. Dashboard: http://localhost:9090/dashboard"
