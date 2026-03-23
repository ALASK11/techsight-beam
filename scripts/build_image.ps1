param(
    [Parameter(Mandatory=$true)][string]$Project,
    [Parameter(Mandatory=$true)][string]$Region
)

$ErrorActionPreference = "Stop"

$Image = "${Region}-docker.pkg.dev/${Project}/techsight-beam/techsight-beam:latest"

Write-Host "Configuring Docker to authenticate with Artifact Registry..."
gcloud auth configure-docker "${Region}-docker.pkg.dev" --quiet

Write-Host "Building custom container image: $Image"
docker build -t $Image .

Write-Host "Pushing image..."
docker push $Image

Write-Host "Done."
