variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  description = "The GCP region for resources (e.g., us-central1)"
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket for Dataflow temp/staging and input files. Must be globally unique."
  type        = string
}

variable "bq_dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "techsight"
}

variable "bq_table_id" {
  description = "BigQuery table ID for script counts"
  type        = string
  default     = "script_counts"
}

variable "artifact_registry_repo" {
  description = "Name for the Artifact Registry Docker repository"
  type        = string
  default     = "techsight-beam"
}
