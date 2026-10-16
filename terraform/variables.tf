variable "environment" {
  description = "Deployment environment (dev/staging/prod)."
  type        = string
}

variable "databricks_host" {
  description = "Databricks workspace URL."
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token (from secret manager)."
  type        = string
  sensitive   = true
}

variable "databricks_warehouse_id" {
  description = "SQL Warehouse id for dbt + Power BI."
  type        = string
}

variable "databricks_cluster_size" {
  description = "Databricks SQL Warehouse cluster size."
  type        = string
  default     = "Medium"
}

variable "kafka_broker" {
  description = "Kafka bootstrap server(s)."
  type        = string
}

variable "kafka_security_protocol" {
  description = "PLAINTEXT (dev) or SASL_SSL / SSL (staging/prod)."
  type        = string
  default     = "PLAINTEXT"
}

variable "kafka_sasl_username" {
  description = "Kafka SASL username (empty for PLAINTEXT)."
  type        = string
  default     = ""
  sensitive   = true
}

variable "kafka_sasl_password" {
  description = "Kafka SASL password (empty for PLAINTEXT)."
  type        = string
  default     = ""
  sensitive   = true
}

variable "kafka_retention_ms" {
  description = "CDC topic retention in ms (default 7 days)."
  type        = number
  default     = 604800000
}

variable "kafka_partitions_orders" {
  description = "Partition count for high-churn order topics."
  type        = number
  default     = 8
}

variable "kafka_partitions_default" {
  description = "Partition count for other CDC topics."
  type        = number
  default     = 4
}

variable "kafka_replication_factor" {
  description = "Topic replication factor (>= 2 for staging/prod)."
  type        = number
  default     = 1
}

variable "catalog_name" {
  description = "Unity Catalog name for retailsync schemas."
  type        = string
  default     = "retailsync"
}