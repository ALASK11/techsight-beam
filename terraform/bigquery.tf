resource "google_bigquery_dataset" "techsight_dataset" {
  dataset_id  = var.bq_dataset_id
  description = "Dataset for TechSight Common Crawl analysis"
  location    = var.region

  depends_on = [
    google_project_service.bigquery
  ]
}

resource "google_bigquery_table" "script_counts" {
  dataset_id = google_bigquery_dataset.techsight_dataset.dataset_id
  table_id   = var.bq_table_id

  schema = <<EOF
[
  {
    "name": "crawl_date",
    "type": "DATE",
    "mode": "REQUIRED",
    "description": "WARC-Date truncated to YYYY-MM-DD"
  },
  {
    "name": "page_url",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Target URL that matched the filter"
  },
  {
    "name": "script_origin",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Hostname of the resolved <script src> URL"
  },
  {
    "name": "count",
    "type": "INTEGER",
    "mode": "REQUIRED",
    "description": "Number of <script> tags from that origin on that page"
  }
]
EOF

  depends_on = [
    google_project_service.bigquery
  ]
}
