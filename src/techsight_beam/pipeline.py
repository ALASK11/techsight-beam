"""Core pipeline construction for Common Crawl script tag extraction.

Uses the CC Index (Parquet on S3/GCS) to look up which WARC records contain
target URLs, then fetches only those records via HTTP byte-range requests.
This is orders of magnitude cheaper than scanning all ~90 K WARC files.
"""

import logging

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToBigQuery, BigQueryDisposition

from techsight_beam.transforms.aggregate import FormatForBigQuery
from techsight_beam.transforms.cc_index import (
    BuildFrozenSetCombineFn,
    ReadCCIndexPartitionFn,
    list_cc_index_files,
)
from techsight_beam.transforms.read_warc import (
    FetchWarcRecordFn,
    FilterByTargetURLFn,
)
from techsight_beam.transforms.script_extractor import ExtractScriptsFn
from techsight_beam.utils.url_normalize import (
    extract_registered_domain,
    normalize_url,
)

logger = logging.getLogger(__name__)

BQ_SCHEMA = {
    "fields": [
        {"name": "crawl_date", "type": "STRING", "mode": "REQUIRED"},
        {"name": "page_url", "type": "STRING", "mode": "REQUIRED"},
        {"name": "script_origin", "type": "STRING", "mode": "REQUIRED"},
        {"name": "count", "type": "INTEGER", "mode": "REQUIRED"},
    ]
}


def build_pipeline(pipeline: beam.Pipeline, options) -> beam.Pipeline:
    """Construct the full pipeline graph.

    Steps
    -----
    1. Read target URLs -> extract registered domains -> build domain side input.
    2. Read CC Index Parquet partitions, filter by domain + ``text/html``.
    3. Exact-match filter against the full target URL set.
    4. Fetch matching WARC records via HTTP byte-range requests.
    5. Parse HTML and extract ``<script src>`` tags.
    6. Count per ``(crawl_date, page_url, script_origin)`` composite key.
    7. Write results to BigQuery.
    """

    # ── Discover CC Index partitions (runs at submission time) ───────────
    parquet_files = list_cc_index_files(
        crawl_id=options.crawl_id,
        cc_index_base=options.cc_index_base,
    )
    logger.info("Will process %d CC Index partitions", len(parquet_files))

    # ── Step 1: Target URLs -> domain side input ────────────────────────
    target_urls = (
        pipeline
        | "ReadTargetURLs" >> ReadFromText(options.target_urls_path)
        | "NormalizeURLs" >> beam.Map(normalize_url)
    )

    domain_set = (
        target_urls
        | "ExtractDomains" >> beam.Map(extract_registered_domain)
        | "BuildDomainSet" >> beam.CombineGlobally(BuildFrozenSetCombineFn())
    )

    # ── Step 2: Read CC Index, filter by domain ─────────────────────────
    cc_entries = (
        pipeline
        | "CreateParquetPaths" >> beam.Create(parquet_files)
        | "ReadCCIndex"
        >> beam.ParDo(
            ReadCCIndexPartitionFn(),
            domain_set=beam.pvalue.AsSingleton(domain_set),
        )
    )

    # ── Step 3: Exact URL filter ────────────────────────────────────────
    matched = (
        cc_entries
        | "FilterByURL"
        >> beam.ParDo(
            FilterByTargetURLFn(target_urls_path=options.target_urls_path)
        )
        | "ReshuffleBeforeFetch" >> beam.Reshuffle()
    )

    # ── Step 4: Fetch WARC records (byte-range) ─────────────────────────
    html_records = matched | "FetchWarcRecords" >> beam.ParDo(
        FetchWarcRecordFn(cc_base_url=options.cc_base_url)
    )

    # ── Step 5: Extract <script src="..."> tags ─────────────────────────
    script_entries = html_records | "ExtractScripts" >> beam.ParDo(
        ExtractScriptsFn()
    )

    # ── Step 6: Aggregate counters ──────────────────────────────────────
    counts = (
        script_entries
        | "CompositeKey"
        >> beam.Map(
            lambda e: ((e["crawl_date"], e["page_url"], e["script_origin"]), 1)
        )
        | "CountPerKey" >> beam.CombinePerKey(sum)
    )

    # ── Step 7: Write to BigQuery ───────────────────────────────────────
    (
        counts
        | "FormatRows" >> beam.ParDo(FormatForBigQuery())
        | "WriteToBigQuery"
        >> WriteToBigQuery(
            table=options.output_table,
            schema=BQ_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        )
    )

    return pipeline
