"""Transform for formatting aggregated counter results into BigQuery rows."""

import apache_beam as beam


class FormatForBigQuery(beam.DoFn):
    """Converts ``((crawl_date, page_url, script_origin), count)`` tuples
    into BigQuery-compatible row dicts."""

    def process(self, element):
        (crawl_date, page_url, script_origin), count = element
        yield {
            "crawl_date": crawl_date,
            "page_url": page_url,
            "script_origin": script_origin,
            "count": count,
        }
