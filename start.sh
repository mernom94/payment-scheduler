#!/bin/bash
alembic upgrade head & 
python workers.scheduler &
python workers.webhook &
python workers.reconciliation &
uvicorn main:app --host 0.0.0.0 --port $PORT