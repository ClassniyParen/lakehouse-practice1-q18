#!/bin/sh
set -eu
cd "$(dirname "$0")"
command -v docker >/dev/null
docker info >/dev/null
mkdir -p results/trino
rm -f results/bi-bronze.json
docker compose config --quiet
docker compose up -d trino
docker compose --profile tools run --rm runner python scripts/run_trino.py --sf 1 --repeats 5
docker compose --profile bi up -d --build superset
attempt=0
until test -f results/bi-bronze.json; do
  attempt=$((attempt+1))
  if test "$attempt" -ge 200; then echo 'BI verification timed out'; exit 1; fi
  sleep 3
done
docker compose ps --all > results/trino/docker-ps.txt
docker compose images > results/trino/docker-images.txt
docker compose logs --no-color > results/trino/docker.log
docker compose --profile tools run --rm runner python scripts/make_report.py --engine trino
