#!/bin/bash
alembic upgrade head & 
python app.workers.scheduler &
python app.workers.webhook &
python app.workers.reconciliation &
uvicorn main:app --host 0.0.0.0 --port $PORT