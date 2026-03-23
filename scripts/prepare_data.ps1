param(
    [Parameter(Mandatory=$true)][string]$GcsBucket,
    [Parameter(Mandatory=$true)][string]$BqProject,
    [Parameter(Mandatory=$true)][string]$TargetUrlsFile
)

$ErrorActionPreference = "Stop"

$Dest = "gs://${GcsBucket}/techsight/target_urls.txt"

Write-Host "Uploading target URLs to ${Dest} ..."
gsutil cp $TargetUrlsFile $Dest

Write-Host "Creating BigQuery dataset ${BqProject}:techsight (if needed) ..."
try {
    bq mk --dataset --project_id=$BqProject "${BqProject}:techsight"
} catch {
    # Dataset already exists, ignore
}

Write-Host ""
Write-Host "Done!  Run the pipeline with:"
Write-Host "  --target_urls_path ${Dest}"
Write-Host "  --output_table ${BqProject}:techsight.script_counts"
