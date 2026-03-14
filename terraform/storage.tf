resource "google_storage_bucket" "techsight_bucket" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  depends_on = [
    google_project_service.storage
  ]
}
