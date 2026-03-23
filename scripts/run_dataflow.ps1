param(
    [Parameter(Mandatory=$true)][string]$Project,
    [Parameter(Mandatory=$true)][string]$Region,
    [Parameter(Mandatory=$true)][string]$Bucket,
    [Parameter(Mandatory=$true)][string]$Crawl,
    [int]$Workers = 200,
    [string]$CcIndexBase = "gs://techsight-cc-columnar-index"
)

$ErrorActionPreference = "Stop"

$Image = "${Region}-docker.pkg.dev/${Project}/techsight-beam/techsight-beam:latest"
$TargetUrls = "gs://${Bucket}/techsight/target_urls.txt"
$OutputTable = "${Project}:techsight.script_counts"

Write-Host "Launching Dataflow pipeline..."
Write-Host "Project: $Project"
Write-Host "Region: $Region"
Write-Host "Bucket: $Bucket"
Write-Host "Crawl ID: $Crawl"
Write-Host "Workers: $Workers"
Write-Host "CC Index: $CcIndexBase"
Write-Host "Image: $Image"

python -m techsight_beam.main `
    --runner DataflowRunner `
    --project $Project `
    --region $Region `
    --temp_location "gs://${Bucket}/tmp" `
    --staging_location "gs://${Bucket}/staging" `
    --setup_file ./setup.py `
    --crawl_id $Crawl `
    --target_urls_path $TargetUrls `
    --output_table $OutputTable `
    --cc_index_base $CcIndexBase `
    --extraction_only `
    --machine_type n1-standard-4 `
    --disk_size_gb 50 `
    --max_num_workers $Workers `
    --sdk_container_image $Image `
    --experiments use_runner_v2

Write-Host "Pipeline submitted successfully."
