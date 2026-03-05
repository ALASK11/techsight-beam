#!/usr/bin/env bash
#
# Prepares data for the TechSight pipeline:
#   1. Uploads target URLs to GCS.
#   2. Creates the BigQuery dataset (if it doesn't exist).
#
# The CC Index is read directly from S3 at runtime (no mirroring needed).
#
# Usage:
#   ./scripts/prepare_data.sh <GCS_BUCKET> <BQ_PROJECT> <TARGET_URLS_FILE>
#
# Example:
#   ./scripts/prepare_data.sh my-bucket my-project target_urls.txt
#
set -euo pipefail

GCS_BUCKET="${1:?Usage: $0 <GCS_BUCKET> <BQ_PROJECT> <TARGET_URLS_FILE>}"
BQ_PROJECT="${2:?Usage: $0 <GCS_BUCKET> <BQ_PROJECT> <TARGET_URLS_FILE>}"
TARGET_FILE="${3:?Usage: $0 <GCS_BUCKET> <BQ_PROJECT> <TARGET_URLS_FILE>}"

DEST="gs://${GCS_BUCKET}/techsight/target_urls.txt"

echo "Uploading target URLs to ${DEST} ..."
gsutil cp "${TARGET_FILE}" "${DEST}"

echo "Creating BigQuery dataset ${BQ_PROJECT}:techsight (if needed) ..."
bq mk --dataset --project_id="${BQ_PROJECT}" "${BQ_PROJECT}:techsight" 2>/dev/null || true

echo ""
echo "Done!  Run the pipeline with:"
echo "  --target_urls_path ${DEST}"
echo "  --output_table ${BQ_PROJECT}:techsight.script_counts"
