terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.8"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = "us-central1"
}

# GCS Bucket - landing zone
resource "google_storage_bucket" "landing_zone" {
  name          = "${var.gcp_project_id}-landing"
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# BigQuery Dataset - Bronze Layer
resource "google_bigquery_dataset" "bronze" {
  dataset_id                 = "bronze"
  location                   = var.location
  description                = "Raw ingested data with JSON blobs"
  delete_contents_on_destroy = true
}
