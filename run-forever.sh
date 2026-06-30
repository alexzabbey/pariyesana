#!/usr/bin/env bash
# ponytail: self-heal supervisor. `main.py run` opens the SSH tunnel once and crashes
# on the next DB call when it drops (every 1-2h). Restart it instead of staying dead.
cd "$(dirname "$0")"
while true; do
  TS=$(date +%Y%m%d-%H%M%S)
  LOG="logs/run-$TS.log"
  echo "$LOG" > logs/current.log
  echo "$(date '+%F %T') | SUPERVISE | start -> $LOG" >> logs/supervisor.log
  PYTHONUNBUFFERED=1 uv run main.py run >> "$LOG" 2>&1
  echo "$(date '+%F %T') | SUPERVISE | exited code=$?, restart in 10s" >> logs/supervisor.log
  sleep 10
done
