"""Tests for the script tag extraction transform."""

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from techsight_beam.transforms.script_extractor import ExtractScriptsFn


class TestExtractScriptsFn:
    def test_extracts_external_scripts(self):
        record = {
            "url": "https://example.com/page",
            "crawl_date": "2024-03-15",
            "html": (
                "<html><head>"
                '<script src="https://cdn.jquery.com/jquery.min.js"></script>'
                '<script src="https://analytics.google.com/ga.js"></script>'
                "</head><body>"
                "<script>var x = 1;</script>"
                '<script src="/js/app.js"></script>'
                "</body></html>"
            ),
        }

        with TestPipeline() as p:
            output = p | beam.Create([record]) | beam.ParDo(ExtractScriptsFn())

            expected = [
                {
                    "crawl_date": "2024-03-15",
                    "page_url": "https://example.com/page",
                    "script_origin": "cdn.jquery.com",
                },
                {
                    "crawl_date": "2024-03-15",
                    "page_url": "https://example.com/page",
                    "script_origin": "analytics.google.com",
                },
                {
                    "crawl_date": "2024-03-15",
                    "page_url": "https://example.com/page",
                    "script_origin": "example.com",
                },
            ]

            assert_that(output, equal_to(expected))

    def test_ignores_inline_scripts(self):
        record = {
            "url": "https://example.com",
            "crawl_date": "2024-03-15",
            "html": '<html><body><script>alert("hi")</script></body></html>',
        }

        with TestPipeline() as p:
            output = p | beam.Create([record]) | beam.ParDo(ExtractScriptsFn())
            assert_that(output, equal_to([]))

    def test_resolves_relative_urls(self):
        record = {
            "url": "https://example.com/dir/page",
            "crawl_date": "2024-01-01",
            "html": '<html><script src="../static/app.js"></script></html>',
        }

        with TestPipeline() as p:
            output = p | beam.Create([record]) | beam.ParDo(ExtractScriptsFn())
            expected = [
                {
                    "crawl_date": "2024-01-01",
                    "page_url": "https://example.com/dir/page",
                    "script_origin": "example.com",
                },
            ]
            assert_that(output, equal_to(expected))

    def test_handles_protocol_relative_urls(self):
        record = {
            "url": "https://example.com",
            "crawl_date": "2024-01-01",
            "html": '<html><script src="//cdn.example.com/lib.js"></script></html>',
        }

        with TestPipeline() as p:
            output = p | beam.Create([record]) | beam.ParDo(ExtractScriptsFn())
            expected = [
                {
                    "crawl_date": "2024-01-01",
                    "page_url": "https://example.com",
                    "script_origin": "cdn.example.com",
                },
            ]
            assert_that(output, equal_to(expected))

    def test_handles_malformed_html(self):
        record = {
            "url": "https://example.com",
            "crawl_date": "2024-01-01",
            "html": '<html><script src="https://cdn.example.com/app.js"<broken>',
        }

        with TestPipeline() as p:
            output = p | beam.Create([record]) | beam.ParDo(ExtractScriptsFn())
            expected = [
                {
                    "crawl_date": "2024-01-01",
                    "page_url": "https://example.com",
                    "script_origin": "cdn.example.com",
                },
            ]
            assert_that(output, equal_to(expected))

    def test_lowercases_script_origin(self):
        record = {
            "url": "https://example.com",
            "crawl_date": "2024-01-01",
            "html": '<html><script src="https://CDN.Example.COM/app.js"></script></html>',
        }

        with TestPipeline() as p:
            output = p | beam.Create([record]) | beam.ParDo(ExtractScriptsFn())
            expected = [
                {
                    "crawl_date": "2024-01-01",
                    "page_url": "https://example.com",
                    "script_origin": "cdn.example.com",
                },
            ]
            assert_that(output, equal_to(expected))
