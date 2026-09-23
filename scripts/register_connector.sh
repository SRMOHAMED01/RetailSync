#!/usr/bin/env bash
set -euo pipefail
curl -fsS -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' --data @debezium/connectors/postgres-retail.json || \
curl -fsS -X PUT http://localhost:8083/connectors/retailsync-postgres/config -H 'Content-Type: application/json' --data "$(python -c 'import json; print(json.dumps(json.load(open("debezium/connectors/postgres-retail.json"))["config"]))')"
echo; curl -fsS http://localhost:8083/connectors/retailsync-postgres/status; echo
