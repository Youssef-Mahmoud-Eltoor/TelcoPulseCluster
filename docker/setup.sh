#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "[1/5] Setting up local directory permissions..."
mkdir -p superset_home ch_data ch_logs
sudo chmod -R 777 superset_home ch_data ch_logs

echo "[2/5] Starting ClickHouse and Superset containers..."
docker compose -f Clickhouse-compose.yml up -d clickhouse superset

echo "[3/5] Installing ClickHouse Python connector inside Superset..."
docker exec -i --user root superset-node5 pip install --target /app/.venv/lib/python3.10/site-packages clickhouse-connect
docker exec -i --user root superset-node5 chmod -R 755 /app/.venv/lib/python3.10/site-packages

echo "[4/5] Running Superset database migrations and creating admin..."
docker exec -i superset-node5 superset db upgrade
docker exec -i superset-node5 superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@telcopulse.local \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true
docker exec -i superset-node5 superset init

if [ -s ../Superset/provision_dashboard.py ]; then
  echo "[5/5] Provisioning visual charts and dashboard..."
  docker exec -i superset-node5 python < ../Superset/provision_dashboard.py
else
  echo "[5/5] Skipping dashboard provisioning: ../Superset/provision_dashboard.py is empty."
fi

echo "=========================================================="
echo " TelcoPulse Analytics Stack is live!"
echo " ClickHouse Native: 127.0.0.1:9000 (HTTP: 8123)"
echo " Superset UI:       http://localhost:8088/superset/dashboard/telcopulse-ops/"
echo " Credentials:       admin / ${SUPERSET_ADMIN_PASSWORD:-admin}"
echo "=========================================================="
