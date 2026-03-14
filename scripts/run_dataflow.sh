#!/bin/bash
set -e

if [ "$#" -lt 4 ]; then
    echo "Usage: $0 <PROJECT_ID> <REGION> <GCS_BUCKET> <CRAWL_ID> [WORKERS=200] [CC_INDEX_BASE]"
    echo "Example: $0 my-gcp-project us-central1 my-techsight-bucket CC-MAIN-2025-08 100 gs://techsight-cc-columnar-index"
    exit 1
fi

PROJECT=$1
REGION=$2
BUCKET=$3
CRAWL=$4
WORKERS=${5:-200}
CC_INDEX_BASE=${6:-"gs://techsight-cc-columnar-index"}

# Image built by build_image.sh (Artifact Registry)
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/techsight-beam/techsight-beam:latest"

# Default paths based on the README
TARGET_URLS="gs://${BUCKET}/techsight/target_urls.txt"
OUTPUT_TABLE="${PROJECT}:techsight.script_counts"

echo "Launching Dataflow pipeline..."
echo "Project: $PROJECT"
echo "Region: $REGION"
echo "Bucket: $BUCKET"
echo "Crawl ID: $CRAWL"
echo "Workers: $WORKERS"
echo "CC Index: $CC_INDEX_BASE"
echo "Image: $IMAGE"

python -m techsight_beam.main \
    --runner DataflowRunner \
    --project "$PROJECT" \
    --region "$REGION" \
    --temp_location "gs://${BUCKET}/tmp" \
    --staging_location "gs://${BUCKET}/staging" \
    --setup_file ./setup.py \
    --crawl_id "$CRAWL" \
    --target_urls_path "$TARGET_URLS" \
    --output_table "$OUTPUT_TABLE" \
    --cc_index_base "$CC_INDEX_BASE" \
    --extraction_only \
    --machine_type n1-standard-4 \
    --disk_size_gb 50 \
    --max_num_workers "$WORKERS" \
    --sdk_container_image "$IMAGE" \
    --experiments use_runner_v2

echo "Pipeline submitted successfully."
