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

resource "google_storage_bucket" "landing_zone" {
  name          = "${var.gcp_project_id}-landing"
  location      = "US"
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
