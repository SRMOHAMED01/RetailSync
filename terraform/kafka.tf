# CDC topics managed by Terraform.
# Match retention/partions to kafka/topics.yml.

locals {
  cdc_topics = {
    "customers"     = { partitions = var.kafka_partitions_default }
    "products"      = { partitions = var.kafka_partitions_default }
    "orders"        = { partitions = var.kafka_partitions_orders }
    "order_items"   = { partitions = var.kafka_partitions_orders }
    "payments"      = { partitions = var.kafka_partitions_default }
  }
}

resource "kafka_topic" "cdc_topics" {
  for_each = local.cdc_topics

  name             = "retail.public.${each.key}"
  partitions_count = each.value.partitions
  replication_factor = var.kafka_replication_factor

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = var.kafka_retention_ms.to_string()
  }
}

# Kafka Connect internal topics (compact).
resource "kafka_topic" "connect_configs" {
  name                = "retailsync_connect_configs"
  partitions_count    = 1
  replication_factor  = var.kafka_replication_factor
  config              = { "cleanup.policy" = "compact" }
}

resource "kafka_topic" "connect_offsets" {
  name                = "retailsync_connect_offsets"
  partitions_count    = 25
  replication_factor  = var.kafka_replication_factor
  config              = { "cleanup.policy" = "compact" }
}

resource "kafka_topic" "connect_statuses" {
  name                = "retailsync_connect_statuses"
  partitions_count    = 5
  replication_factor  = var.kafka_replication_factor
  config              = { "cleanup.policy" = "compact" }
}