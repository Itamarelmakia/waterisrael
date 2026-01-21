#!/usr/bin/env bash
set -e
export PYTHONPATH=src
uvicorn service_api.main:app --host 0.0.0.0 --port $PORT
