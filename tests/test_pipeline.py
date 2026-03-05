"""Integration test for the extract -> aggregate -> format pipeline stages.

These stages are independent of CC Index / WARC fetching, so we can test
them in isolation with synthetic in-memory data.
"""

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from techsight_beam.transforms.aggregate import FormatForBigQuery
from techsight_beam.transforms.script_extractor import ExtractScriptsFn


class TestPipelineIntegration:
    def test_extract_aggregate_format(self):
        """End-to-end test: extract scripts, count, format for BigQuery."""
        records = [
            {
                "url": "https://example.com",
                "crawl_date": "2024-03-15",
                "html": (
                    "<html>"
                    '<script src="https://cdn.example.com/a.js"></script>'
                    '<script src="https://cdn.example.com/b.js"></script>'
                    '<script src="https://analytics.test.com/t.js"></script>'
                    "</html>"
                ),
            },
            {
                "url": "https://other.com",
                "crawl_date": "2024-03-15",
                "html": (
                    "<html>"
                    '<script src="https://cdn.example.com/a.js"></script>'
                    "</html>"
                ),
            },
        ]

        with TestPipeline() as p:
            output = (
                p
                | beam.Create(records)
                | "Extract" >> beam.ParDo(ExtractScriptsFn())
                | "Key"
                >> beam.Map(
                    lambda e: (
                        (e["crawl_date"], e["page_url"], e["script_origin"]),
                        1,
                    )
                )
                | "Count" >> beam.CombinePerKey(sum)
                | "Format" >> beam.ParDo(FormatForBigQuery())
            )

            expected = [
                {
                    "crawl_date": "2024-03-15",
                    "page_url": "https://example.com",
                    "script_origin": "cdn.example.com",
                    "count": 2,
                },
                {
                    "crawl_date": "2024-03-15",
                    "page_url": "https://example.com",
                    "script_origin": "analytics.test.com",
                    "count": 1,
                },
                {
                    "crawl_date": "2024-03-15",
                    "page_url": "https://other.com",
                    "script_origin": "cdn.example.com",
                    "count": 1,
                },
            ]

            assert_that(output, equal_to(expected))


class TestBuildFrozenSetCombineFn:
    def test_builds_frozenset(self):
        from techsight_beam.transforms.cc_index import BuildFrozenSetCombineFn

        with TestPipeline() as p:
            output = (
                p
                | beam.Create(["a.com", "b.com", "a.com", "c.com"])
                | beam.CombineGlobally(BuildFrozenSetCombineFn())
            )

            assert_that(
                output,
                equal_to([frozenset({"a.com", "b.com", "c.com"})]),
            )
