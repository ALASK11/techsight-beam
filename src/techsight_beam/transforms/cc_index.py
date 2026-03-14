"""Transforms for reading the Common Crawl columnar index (cc-index).

The CC Index is stored as partitioned Parquet on S3:

    s3://commoncrawl/cc-index/table/cc-main/warc/crawl=<CRAWL_ID>/subset=warc/*.parquet

Each partition file contains columns such as ``url``,
``url_host_registered_domain``, ``warc_filename``, ``warc_record_offset``,
``warc_record_length``, ``fetch_time``, and ``content_mime_detected``.

This module provides:

* :func:`list_cc_index_files` — enumerate Parquet partitions for a crawl
  (runs at pipeline-construction time).
* :class:`ReadCCIndexPartitionFn` — DoFn that reads one Parquet file,
  filters rows to target domains and ``text/html``, and yields index entries.
* :class:`BuildFrozenSetCombineFn` — ``CombineFn`` that accumulates
  distinct strings into a ``frozenset`` (used to build the domain side input).
"""

import logging

import apache_beam as beam
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

CC_INDEX_S3_BASE = "s3://commoncrawl/cc-index/table/cc-main/warc"

# Columns we read from the CC Index; everything else is pruned at the Parquet
# level so we transfer only a fraction of the full ~300 GB per crawl.
CC_INDEX_COLUMNS = [
    "url",
    "url_host_registered_domain",
    "warc_filename",
    "warc_record_offset",
    "warc_record_length",
    "fetch_time",
    "content_mime_detected",
]

# Columns forwarded to the rest of the pipeline (after filtering).
_OUTPUT_COLUMNS = [
    "url",
    "warc_filename",
    "warc_record_offset",
    "warc_record_length",
    "fetch_time",
]


# ── Filesystem helpers ───────────────────────────────────────────────────


def _resolve_filesystem(uri: str):
    """Return ``(pyarrow FileSystem, path_without_scheme)`` for a URI."""
    if uri.startswith("s3://"):
        return pafs.S3FileSystem(anonymous=True, region="us-east-1"), uri[5:]
    if uri.startswith("gs://"):
        return pafs.GcsFileSystem(), uri[5:]
    return pafs.LocalFileSystem(), uri


# ── Partition listing (pipeline-construction time) ───────────────────────


def list_cc_index_files(cc_index_base: str) -> list[str]:
    """Return fully-qualified URIs of every Parquet partition in the generic directory.

    Runs on the machine that **constructs** the pipeline (i.e. the submitter),
    not on Dataflow workers.  The result is passed to ``beam.Create``.
    """
    base = cc_index_base.rstrip("/")

    # Determine the scheme so we can re-attach it to each returned path.
    scheme = ""
    if "://" in base:
        scheme = base.split("://")[0] + "://"

    fs, prefix = _resolve_filesystem(base)
    selector = pafs.FileSelector(prefix, recursive=True)

    try:
        infos = fs.get_file_info(selector)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to list CC Index files at {base}: {exc}. "
        ) from exc

    paths = sorted(
        f"{scheme}{info.path}"
        for info in infos
        if info.type == pafs.FileType.File and (info.path.endswith(".parquet") or "part-" in info.path.split("/")[-1])
    )
    logger.info("Found %d CC Index partitions at %s", len(paths), base)
    return paths


# ── Parquet reader DoFn ──────────────────────────────────────────────────


class ReadCCIndexPartitionFn(beam.DoFn):
    """Reads one CC Index Parquet file and yields entries whose registered
    domain is in the target set **and** whose detected MIME type is
    ``text/html``.

    Input
        S3/GCS URI to a single Parquet file.
    Side input
        ``domain_set`` — ``frozenset[str]`` of target registered domains.
    Output
        ``dict`` with keys ``url``, ``warc_filename``, ``warc_record_offset``,
        ``warc_record_length``, ``fetch_time``.
    """

    PARTITIONS_READ = beam.metrics.Metrics.counter("cc_index", "partitions_read")
    ROWS_SCANNED = beam.metrics.Metrics.counter("cc_index", "rows_scanned")
    ROWS_MATCHED = beam.metrics.Metrics.counter("cc_index", "rows_matched")

    def __init__(self):
        self._s3_fs = None
        self._gcs_fs = None

    def _get_fs_and_path(self, uri: str):
        """Return a cached filesystem + stripped path."""
        if uri.startswith("s3://"):
            if self._s3_fs is None:
                self._s3_fs = pafs.S3FileSystem(anonymous=True, region="us-east-1")
            return self._s3_fs, uri[5:]
        if uri.startswith("gs://"):
            if self._gcs_fs is None:
                self._gcs_fs = pafs.GcsFileSystem()
            return self._gcs_fs, uri[5:]
        return pafs.LocalFileSystem(), uri

    def process(self, parquet_uri: str, domain_set):
        fs, resolved = self._get_fs_and_path(parquet_uri)

        try:
            table = pq.read_table(
                resolved, filesystem=fs, columns=CC_INDEX_COLUMNS
            )
        except Exception as exc:
            logger.warning("Failed to read %s: %s", parquet_uri, exc)
            return

        total = table.num_rows

        # ── Filter: content_mime_detected == "text/html" ─────────────────
        if "content_mime_detected" in table.column_names:
            table = table.filter(
                pc.equal(table["content_mime_detected"], "text/html")
            )

        # ── Filter: url_host_registered_domain ∈ domain_set ─────────────
        domain_values = pa.array(list(domain_set), type=pa.string())
        table = table.filter(
            pc.is_in(
                table["url_host_registered_domain"], value_set=domain_values
            )
        )

        matched = table.num_rows

        # ── Yield matching rows ──────────────────────────────────────────
        for batch in table.select(_OUTPUT_COLUMNS).to_batches(
            max_chunksize=10_000
        ):
            cols = batch.to_pydict()
            for i in range(batch.num_rows):
                yield {col: cols[col][i] for col in _OUTPUT_COLUMNS}

        self.PARTITIONS_READ.inc()
        self.ROWS_SCANNED.inc(total)
        self.ROWS_MATCHED.inc(matched)
        logger.info(
            "CC Index %s: %d scanned, %d matched",
            parquet_uri.rsplit("/", 1)[-1],
            total,
            matched,
        )


# ── Combine helper ───────────────────────────────────────────────────────


class BuildFrozenSetCombineFn(beam.CombineFn):
    """Accumulates distinct strings into a ``frozenset``.

    Used to build the registered-domain side input from the target URL list.
    """

    def create_accumulator(self):
        return set()

    def add_input(self, accumulator, element):
        accumulator.add(element)
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = set()
        for acc in accumulators:
            merged.update(acc)
        return merged

    def extract_output(self, accumulator):
        return frozenset(accumulator)
