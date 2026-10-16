#!/usr/bin/env bash
# Create the RetailSync Kafka topics explicitly with production-grade config.
#
# Usage: bash scripts/create_topics.sh [bootstrap-server]
# Defaults to the local compose stack (kafka:29092 inside the network).
set -euo pipefail

BOOTSTRAP="${1:-kafka:29092}"
CONTAINER="retailsync-kafka"

docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic retail.public.customers \
  --partitions 4 --replication-factor 1 \
  --config retention.ms=604800000 --config cleanup.policy=delete

docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic retail.public.products \
  --partitions 4 --replication-factor 1 \
  --config retention.ms=604800000 --config cleanup.policy=delete

docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic retail.public.orders \
  --partitions 8 --replication-factor 1 \
  --config retention.ms=604800000 --config cleanup.policy=delete

docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic retail.public.order_items \
  --partitions 8 --replication-factor 1 \
  --config retention.ms=604800000 --config cleanup.policy=delete

docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic retail.public.payments \
  --partitions 4 --replication-factor 1 \
  --config retention.ms=604800000 --config cleanup.policy=delete

echo "Topics created/verified on $BOOTSTRAP"
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --list