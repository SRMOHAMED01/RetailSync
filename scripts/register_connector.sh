#!/usr/bin/env bash
# Register (or update) the RetailSync Debezium PostgreSQL connector.
#
# Usage: bash scripts/register_connector.sh [connect-url]
# Defaults to the local compose stack (http://localhost:8083).
set -euo pipefail

CONNECT_URL="${1:-http://localhost:8083}"
CONFIG_FILE="debezium/connectors/postgres-retail.json"
CONNECTOR_NAME="retailsync-postgres"

# POST if absent, PUT (update config) if already registered.
if curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" >/dev/null 2>&1; then
  echo "Updating existing connector $CONNECTOR_NAME..."
  curl -fsS -X PUT "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" \
    -H 'Content-Type: application/json' \
    --data "$(python3 -c "import json; print(json.dumps(json.load(open('$CONFIG_FILE'))['config']))")"
else
  echo "Registering new connector $CONNECTOR_NAME..."
  curl -fsS -X POST "$CONNECT_URL/connectors" \
    -H 'Content-Type: application/json' \
    --data "@$CONFIG_FILE"
fi

echo
echo "--- Status ---"
curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"; echo