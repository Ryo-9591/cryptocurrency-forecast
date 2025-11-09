#!/bin/bash
set -euo pipefail

if [ -n "${MLFLOW_BACKEND_STORE_URI:-}" ]; then
  echo "[entrypoint] Waiting for backend store ${MLFLOW_BACKEND_STORE_URI}..."
  python - "$MLFLOW_BACKEND_STORE_URI" <<'PY'
import sys
import time
from sqlalchemy import create_engine

uri = sys.argv[1]
for attempt in range(30):
    try:
        engine = create_engine(uri)
        with engine.connect():
            pass
        break
    except Exception as exc:
        time.sleep(3)
else:
    raise RuntimeError("Could not connect to backend store")
PY

  echo "[entrypoint] Ensuring initial MLflow tables exist..."
  python - "$MLFLOW_BACKEND_STORE_URI" <<'PY'
import sys
from sqlalchemy import create_engine
from mlflow.store.db.utils import _initialize_tables

uri = sys.argv[1]
engine = create_engine(uri)
_initialize_tables(engine)
PY

  echo "[entrypoint] Applying MLflow database migrations..."
  mlflow db upgrade "$MLFLOW_BACKEND_STORE_URI"
fi

if [ -n "${MODEL_NAME:-}" ]; then
  echo "[entrypoint] Checking availability of model ${MODEL_NAME} (stage: ${MODEL_STAGE:-})..."
  if ! python - "$MODEL_NAME" "${MODEL_STAGE:-}" <<'PY'
import sys
import os
from mlflow.tracking import MlflowClient
from mlflow.exceptions import RestException

model_name = sys.argv[1]
stage = sys.argv[2] or None

tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
client = MlflowClient(tracking_uri=tracking_uri)

def model_available():
    try:
        stages = [stage] if stage else None
        versions = client.get_latest_versions(model_name, stages)
        return bool(versions)
    except RestException:
        return False

sys.exit(0 if model_available() else 1)
PY
  then
    echo "[entrypoint] Model ${MODEL_NAME} (stage: ${MODEL_STAGE:-}) not found. Skipping service startup."
    exit 0
  fi
fi

exec "$@"

