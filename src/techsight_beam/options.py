"""Custom pipeline options for the TechSight Common Crawl pipeline."""

from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam


class TechSightOptions(PipelineOptions):
    """Pipeline-specific arguments for Common Crawl script extraction."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--crawl_id",
            required=True,
            help="Common Crawl crawl identifier (e.g. CC-MAIN-2025-08).",
        )
        parser.add_argument(
            "--target_urls_path",
            required=True,
            help=(
                "Path to a newline-delimited file of target URLs to filter on. "
                "Supports gs:// and local paths."
            ),
        )
        parser.add_argument(
            "--output_table",
            required=True,
            help=(
                "BigQuery output table in the form PROJECT:DATASET.TABLE "
                "(e.g. my-project:techsight.script_counts)."
            ),
        )
        parser.add_argument(
            "--cc_index_base",
            default="gs://techsight-cc-columnar-index",
            help=(
                "Base path to CC Index Parquet files (e.g., gs://my-bucket). "
                "Must point to flat files like part-* or *.parquet"
            ),
        )
        parser.add_argument(
            "--cc_base_url",
            default="https://data.commoncrawl.org/",
            help=(
                "Base URL for fetching WARC record byte ranges. "
                "Change this if you mirror WARC files to GCS."
            ),
        )
        parser.add_argument(
            "--extraction_only",
            action="store_true",
            default=True,
            help="If true, bypass WARC downloads and only output a Parquet file of matched target URLs and their WARC files.",
        )
        parser.add_argument(
            "--extraction_output_prefix",
            default="gs://techsight-cc-columnar-index/extracted_",
            help="Prefix for the output Parquet file when running in extraction_only mode.",
        )
