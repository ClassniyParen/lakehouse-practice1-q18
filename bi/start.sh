#!/bin/sh
set -eu
superset db upgrade
python /app/local/setup.py
superset init
exec superset run -h 0.0.0.0 -p 8088 --with-threads
