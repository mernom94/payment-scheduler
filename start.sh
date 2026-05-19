#!/bin/bash
alembic upgrade head & 
python -m app.workers.scheduler &
python -m app.workers.executor &
python -m app.workers.recovery &
uvicorn app.main:app --host 0.0.0.0 --port $PORT