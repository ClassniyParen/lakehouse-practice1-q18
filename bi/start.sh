#!/bin/sh
set -eu
superset db upgrade
superset init
python /app/local/setup.py
exec superset run -h 0.0.0.0 -p 8088 --with-threads
