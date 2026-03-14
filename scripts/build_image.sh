#!/bin/bash
set -e

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <PROJECT_ID> <REGION>"
    echo "Example: $0 my-gcp-project us-central1"
    exit 1
fi

PROJECT=$1
REGION=$2
# Using Artifact Registry instead of GCR
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/techsight-beam/techsight-beam:latest"

echo "Configuring Docker to authenticate with Artifact Registry..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

echo "Building custom container image: $IMAGE"
docker build -t "$IMAGE" .

echo "Pushing image..."
docker push "$IMAGE"

echo "Done."
