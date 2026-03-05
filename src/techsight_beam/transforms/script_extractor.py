"""Transform for extracting external <script src="..."> tags from HTML records."""

import logging
from urllib.parse import urljoin, urlparse

import apache_beam as beam
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class ExtractScriptsFn(beam.DoFn):
    """Parses HTML and yields one element per external ``<script>`` tag.

    Input: ``{"url": str, "crawl_date": str, "html": str}``
    Output: ``{"crawl_date": str, "page_url": str, "script_origin": str}``

    ``script_origin`` is the hostname of the resolved script ``src`` URL.
    Inline scripts (no ``src``) are ignored.
    """

    PAGES_PROCESSED = beam.metrics.Metrics.counter("scripts", "pages_processed")
    SCRIPTS_FOUND = beam.metrics.Metrics.counter("scripts", "scripts_found")
    PARSE_ERRORS = beam.metrics.Metrics.counter("scripts", "parse_errors")

    def process(self, record):
        page_url = record["url"]
        crawl_date = record["crawl_date"]
        html = record["html"]

        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as exc:
            logger.debug("Failed to parse HTML from %s: %s", page_url, exc)
            self.PARSE_ERRORS.inc()
            return

        scripts_found = 0

        for tag in soup.find_all("script", src=True):
            src = tag.get("src", "").strip()
            if not src:
                continue

            try:
                absolute_src = urljoin(page_url, src)
            except Exception:
                continue

            try:
                parsed = urlparse(absolute_src)
                origin = parsed.hostname or ""
            except Exception:
                continue

            if not origin:
                continue

            scripts_found += 1
            yield {
                "crawl_date": crawl_date,
                "page_url": page_url,
                "script_origin": origin.lower(),
            }

        self.PAGES_PROCESSED.inc()
        self.SCRIPTS_FOUND.inc(scripts_found)
