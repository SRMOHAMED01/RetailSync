terraform {
  required_version = ">= 1.6"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
    kafka = {
      source  = "kafka/kafka"
      version = "~> 1.1"
    }
  }
}

provider "databricks" {
  host = var.databricks_host
  token = var.databricks_token
}

provider "kafka" {
  bootstrap_servers = var.kafka_broker
  sasl_username     = var.kafka_sasl_username
  sasl_password     = var.kafka_sasl_password
  sasl_mechanism    = "PLAIN"
  security_protocol = var.kafka_security_protocol
  tls_enabled       = var.kafka_security_protocol != "PLAINTEXT"
}