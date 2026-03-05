# TechSight Beam — Common Crawl Script Tag Pipeline

Apache Beam pipeline that uses the **CC Index** (columnar Parquet on S3) to
locate target URLs inside Common Crawl, fetches only the matching WARC records
via HTTP byte-range requests, extracts every external `<script src="…">` tag,
and writes per-origin counters to BigQuery.

## Architecture

```
target_urls.txt
     │
     ├──► NormalizeURLs
     │
     ├──► ExtractDomains ──► BuildDomainSet (side input)
     │                             │
     │       CC Index Parquet      ▼
     │       (S3 / GCS)    ┌─────────────────────┐
     │           │          │ ReadCCIndexPartition │
     │           └─────────►│ filter: domain ∈ set │
     │                      │ filter: text/html    │
     │                      └──────────┬────────────┘
     │                                 │
     │                                 ▼
     │                      ┌─────────────────────┐
     └──► target URL set ──►│ FilterByTargetURLFn  │
                            │ (exact URL match)    │
                            └──────────┬────────────┘
                                       │ {url, warc_filename, offset, length}
                                       ▼
                            ┌─────────────────────┐
                            │ FetchWarcRecordFn    │
                            │ (HTTP Range request) │
                            └──────────┬────────────┘
                                       │ {url, crawl_date, html}
                                       ▼
                            ┌─────────────────────┐
                            │ ExtractScriptsFn     │
                            │ (BeautifulSoup)      │
                            └──────────┬────────────┘
                                       │ {crawl_date, page_url, script_origin}
                                       ▼
                            ┌─────────────────────┐
                            │ CombinePerKey(sum)   │
                            └──────────┬────────────┘
                                       ▼
                            ┌─────────────────────┐
                            │ WriteToBigQuery      │
                            └─────────────────────┘
```

### Why the CC Index approach?

The CC Index is a columnar (Parquet) catalog of every URL in a crawl, stored
at `s3://commoncrawl/cc-index/table/cc-main/warc/`.  By reading the index
first and filtering on `url_host_registered_domain` + exact URL, we identify
the precise WARC file, byte offset, and byte length for each target record.
We then fetch **only those bytes** via HTTP `Range` requests — no need to
stream all ~90,000 × 1 GB WARC files.

Conceptually this is the SQL equivalent of:

```sql
SELECT url, warc_filename, warc_record_offset, warc_record_length
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2025-08'
  AND url_host_registered_domain = 'example.com'
  AND content_mime_detected = 'text/html'
```

### Composite counter key

| Column | Description |
|---|---|
| `crawl_date` | WARC-Date truncated to `YYYY-MM-DD` |
| `page_url` | Target URL that matched the filter |
| `script_origin` | Hostname of the resolved `<script src>` URL |
| `count` | Number of `<script>` tags from that origin on that page |

---

## Prerequisites

| Tool | Version |
|---|---|
| Python | >= 3.10 |
| Google Cloud SDK (`gcloud`) | latest |
| Docker (optional, for custom container) | >= 24 |

Enable the following GCP APIs:

```bash
gcloud services enable \
    dataflow.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com
```

---

## Setup

```bash
git clone <repo-url> && cd techsight-beam

python -m venv .venv && source .venv/bin/activate

pip install -e ".[dev]"
```

---

## Prepare data

### 1. Target URLs file

Create a newline-delimited text file with up to ~20 M URLs and upload to GCS:

```bash
gsutil cp target_urls.txt gs://MY_BUCKET/techsight/target_urls.txt
```

### 2. BigQuery dataset

```bash
bq mk --dataset MY_PROJECT:techsight
```

Or use the helper script:

```bash
./scripts/prepare_data.sh MY_BUCKET MY_PROJECT target_urls.txt
```

### 3. CC Index (no preparation needed)

The pipeline reads the CC Index directly from the public S3 bucket
(`s3://commoncrawl/cc-index/table/cc-main/warc/`) using anonymous access.
Dataflow workers need outbound internet access to S3 (the default).

If you prefer to mirror the index to GCS, see
[Mirror CC Index to GCS](#mirror-cc-index-to-gcs).

---

## Run locally (DirectRunner)

For a quick smoke test, point at the sample URLs and a small crawl.
The CC Index listing + Parquet reads will happen over the network:

```bash
python -m techsight_beam.main \
    --runner DirectRunner \
    --crawl_id CC-MAIN-2025-08 \
    --target_urls_path data/sample_urls.txt \
    --output_table MY_PROJECT:techsight.script_counts
```

> Tip: For a fully local test, replace the BigQuery write in `pipeline.py`
> with `WriteToText` (see [Modifying the pipeline](#modifying-the-pipeline)).

---

## Deploy to Dataflow

### Standard launch

```bash
export PROJECT=my-gcp-project
export REGION=us-central1
export BUCKET=my-techsight-bucket
export CRAWL=CC-MAIN-2025-08

python -m techsight_beam.main \
    --runner DataflowRunner \
    --project $PROJECT \
    --region $REGION \
    --temp_location gs://$BUCKET/tmp \
    --staging_location gs://$BUCKET/staging \
    --setup_file ./setup.py \
    --crawl_id $CRAWL \
    --target_urls_path gs://$BUCKET/techsight/target_urls.txt \
    --output_table $PROJECT:techsight.script_counts \
    --machine_type n1-standard-4 \
    --disk_size_gb 50 \
    --max_num_workers 200 \
    --experiments use_runner_v2
```

### With a custom container (recommended for large runs)

```bash
export IMAGE=gcr.io/$PROJECT/techsight-beam:latest
docker build -t $IMAGE .
docker push $IMAGE
```

Then add:

```bash
    --sdk_container_image $IMAGE \
    --experiments use_runner_v2
```

---

## Configuration options

| Flag | Required | Default | Description |
|---|---|---|---|
| `--crawl_id` | yes | — | Common Crawl ID, e.g. `CC-MAIN-2025-08` |
| `--target_urls_path` | yes | — | GCS or local path to target URLs file |
| `--output_table` | yes | — | BigQuery table `PROJECT:DATASET.TABLE` |
| `--cc_index_base` | no | `s3://commoncrawl/cc-index/table/cc-main/warc` | Base path to CC Index Parquet partitions. Set to `gs://...` if mirroring. |
| `--cc_base_url` | no | `https://data.commoncrawl.org/` | Base URL for WARC byte-range fetches |

All standard Beam / Dataflow flags are supported.

---

## Modifying the pipeline

### Change the output sink

Replace the BigQuery write in [pipeline.py](src/techsight_beam/pipeline.py):

```python
from apache_beam.io import WriteToText
import json

(
    counts
    | "FormatRows" >> beam.ParDo(FormatForBigQuery())
    | "ToJson" >> beam.Map(json.dumps)
    | "WriteGCS" >> WriteToText("gs://bucket/output/results", file_name_suffix=".json")
)
```

### Add new HTML extraction logic

Create a new DoFn in [transforms/](src/techsight_beam/transforms/) and chain
it after `ExtractScripts`:

```python
class ExtractMetaTagsFn(beam.DoFn):
    def process(self, record):
        # record has keys: url, crawl_date, html
        ...
```

### Mirror CC Index to GCS

Copy the Parquet partitions for your crawl (about 250-300 GB) and pass the GCS
base path:

```bash
# One-time mirror
aws s3 sync --no-sign-request \
    s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2025-08/ \
    gs://MY_BUCKET/cc-index/table/cc-main/warc/crawl=CC-MAIN-2025-08/

# Then run with:
--cc_index_base gs://MY_BUCKET/cc-index/table/cc-main/warc
```

### Process a different crawl

Just change `--crawl_id`.  Available crawl IDs are listed at
<https://index.commoncrawl.org/collinfo.json>.

---

## Testing

```bash
pytest tests/ -v

# With coverage
pytest tests/ -v --cov=techsight_beam --cov-report=term-missing
```

---

## Cost considerations

With the CC Index approach the cost profile is dominated by the **index read**
(Parquet scan) and the **WARC byte-range fetches**, not full WARC file
streaming.

| Phase | Data moved | Notes |
|---|---|---|
| CC Index read | ~40-60 GB (column-pruned) | One-time per crawl, highly parallelizable |
| WARC fetches | ~20 M x avg 50 KB = ~1 TB | Only matched records, via HTTP `Range` |
| BigQuery write | Small | Aggregated rows only |

| Lever | Guidance |
|---|---|
| **Worker count** | `--max_num_workers 200`. Autoscaling handles the rest. |
| **Machine type** | `n1-standard-4` (15 GB RAM) — holds the 20 M URL set (~2 GB). |
| **Spot VMs** | `--experiments=enable_prime` for Dataflow Prime with spot. Reduces cost 60-80%. |
| **Subset first** | Test with a small `target_urls.txt` before running the full 20 M. |
| **Mirror to GCS** | Eliminates cross-cloud egress for the CC Index read. |

---

## Project structure

```
techsight-beam/
├── src/techsight_beam/
│   ├── main.py                # Entry point
│   ├── pipeline.py            # Pipeline graph construction
│   ├── options.py             # Custom PipelineOptions
│   ├── transforms/
│   │   ├── cc_index.py        # CC Index Parquet reader + domain filter
│   │   ├── read_warc.py       # Byte-range WARC fetch + exact URL filter
│   │   ├── script_extractor.py    # <script src> extraction
│   │   └── aggregate.py       # BigQuery row formatting
│   └── utils/
│       └── url_normalize.py   # URL normalization + domain extraction
├── tests/
├── scripts/
│   └── prepare_data.sh        # Upload targets + create BQ dataset
├── data/
│   └── sample_urls.txt        # Sample target URLs for testing
├── Dockerfile                 # Custom Dataflow container
├── setup.py
└── requirements.txt
```
