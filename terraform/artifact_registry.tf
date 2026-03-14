resource "google_artifact_registry_repository" "techsight_repo" {
  location      = var.region
  repository_id = var.artifact_registry_repo
  description   = "Docker repository for TechSight Beam custom container"
  format        = "DOCKER"

  depends_on = [
    google_project_service.artifactregistry
  ]
}
