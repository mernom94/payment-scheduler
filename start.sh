#!/bin/bash
alembic upgrade head & 
python app.workers.scheduler &
python app.workers.executor &
python app.workers.recovery &
uvicorn main:app --host 0.0.0.0 --port $PORT