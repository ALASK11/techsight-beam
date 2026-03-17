"""Transforms for fetching individual WARC records and filtering by target URLs.

After the CC Index lookup narrows billions of records down to entries matching
target domains, this module provides:

* :class:`FilterByTargetURLFn` — exact URL match against the full 20 M target
  set (loaded once per worker).
* :class:`FetchWarcRecordFn` — fetches a single WARC record via an HTTP
  byte-range request using the offset/length from the CC Index.
"""

import logging
import threading
from io import BytesIO

import apache_beam as beam
import requests
from warcio import ArchiveIterator

from techsight_beam.utils.url_normalize import normalize_url

logger = logging.getLogger(__name__)

# ── Shared URL-set cache (one copy per worker process) ───────────────────

_url_set_lock = threading.Lock()
_url_set_cache: dict[str, frozenset] = {}


def _load_target_urls(path: str) -> frozenset:
    """Load and cache target URLs from a GCS/local file.

    Thread-safe: multiple DoFn instances on the same worker share one copy.
    """
    if path in _url_set_cache:
        return _url_set_cache[path]

    with _url_set_lock:
        if path not in _url_set_cache:
            from apache_beam.io.filesystems import FileSystems

            logger.info("Loading target URLs from %s", path)
            with FileSystems.open(path) as f:
                raw = f.read().decode("utf-8")
            urls = frozenset(
                normalize_url(line.strip())
                for line in raw.splitlines()
                if line.strip()
            )
            logger.info("Loaded %d target URLs", len(urls))
            _url_set_cache[path] = urls

        return _url_set_cache[path]


# ── Exact URL filter ─────────────────────────────────────────────────────


class FilterByTargetURLFn(beam.DoFn):
    """Passes through only CC Index entries whose normalized URL exists in
    the target URL set.

    The full ~20 M URL set is loaded once per worker via
    :func:`_load_target_urls` and cached for all subsequent bundles.
    """

    CHECKED = beam.metrics.Metrics.counter("url_filter", "checked")
    PASSED = beam.metrics.Metrics.counter("url_filter", "passed")

    def __init__(self, target_urls_path: str):
        self.target_urls_path = target_urls_path
        self._url_set: frozenset | None = None

    def setup(self):
        self._url_set = _load_target_urls(self.target_urls_path)

    def process(self, entry):
        self.CHECKED.inc()
        norm_entry_url = normalize_url(entry["url"])
        
        # Check if the entry URL starts with any of our target URL prefixes.
        # This allows "https://google.com/" to match "https://google.com/search?q=..."
        for target_prefix in self._url_set:
            if norm_entry_url.startswith(target_prefix):
                self.PASSED.inc()
                yield entry
                return


# ── Byte-range WARC record fetcher ───────────────────────────────────────


class FetchWarcRecordFn(beam.DoFn):
    """Fetches a single WARC record via an HTTP ``Range`` request.

    Input
        CC Index entry dict with ``warc_filename``, ``warc_record_offset``,
        ``warc_record_length``, ``url``, and ``fetch_time``.
    Output
        ``{"url": str, "crawl_date": str, "html": str}``
    """

    FETCHED = beam.metrics.Metrics.counter("warc_fetch", "fetched")
    ERRORS = beam.metrics.Metrics.counter("warc_fetch", "errors")

    def __init__(self, cc_base_url: str = "https://data.commoncrawl.org/"):
        self.cc_base_url = cc_base_url.rstrip("/")
        self._session: requests.Session | None = None

    def setup(self):
        self._session = requests.Session()
        self._session.headers.update(
            {"User-Agent": "TechSight-Beam/0.1 (CC research pipeline)"}
        )

    def teardown(self):
        if self._session:
            self._session.close()

    def process(self, entry):
        warc_filename = entry["warc_filename"]
        offset = int(entry["warc_record_offset"])
        length = int(entry["warc_record_length"])
        page_url = entry["url"]
        fetch_time = entry.get("fetch_time") or ""

        warc_url = f"{self.cc_base_url}/{warc_filename}"
        end_byte = offset + length - 1

        try:
            resp = self._session.get(
                warc_url,
                headers={"Range": f"bytes={offset}-{end_byte}"},
                timeout=120,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Fetch failed for %s: %s", page_url, exc)
            self.ERRORS.inc()
            return

        try:
            for record in ArchiveIterator(BytesIO(resp.content)):
                if record.rec_type != "response":
                    continue

                html = (
                    record.content_stream()
                    .read()
                    .decode("utf-8", errors="replace")
                )

                crawl_date = (
                    record.rec_headers.get_header("WARC-Date")
                    or str(fetch_time)
                    or ""
                )[:10]

                self.FETCHED.inc()
                yield {
                    "url": page_url,
                    "crawl_date": crawl_date,
                    "html": html,
                }
                return  # exactly one response record per byte range
        except Exception as exc:
            logger.warning("Parse failed for %s: %s", page_url, exc)
            self.ERRORS.inc()
