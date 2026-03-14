output "gcs_bucket_url" {
  description = "The GCS bucket URL to use for --temp_location, --staging_location, and --target_urls_path"
  value       = "gs://${google_storage_bucket.techsight_bucket.name}"
}

output "bq_dataset_id" {
  description = "The BigQuery dataset ID"
  value       = google_bigquery_dataset.techsight_dataset.dataset_id
}

output "bq_table_id" {
  description = "The BigQuery table ID"
  value       = google_bigquery_table.script_counts.table_id
}

output "bq_output_table" {
  description = "The full BigQuery table path to use for --output_table"
  value       = "${var.project_id}:${google_bigquery_dataset.techsight_dataset.dataset_id}.${google_bigquery_table.script_counts.table_id}"
}

output "artifact_registry_url" {
  description = "The Artifact Registry repository URL. You can use this instead of gcr.io in your scripts."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repo}"
}
