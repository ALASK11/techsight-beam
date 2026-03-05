"""Tests for the CC Index transform helpers."""

from techsight_beam.transforms.cc_index import (
    BuildFrozenSetCombineFn,
    _resolve_filesystem,
)


class TestResolveFilesystem:
    def test_s3_scheme(self):
        fs, path = _resolve_filesystem("s3://commoncrawl/cc-index/foo")
        assert path == "commoncrawl/cc-index/foo"
        assert "S3" in type(fs).__name__

    def test_local_fallback(self):
        fs, path = _resolve_filesystem("/tmp/data/foo.parquet")
        assert path == "/tmp/data/foo.parquet"
        assert "Local" in type(fs).__name__


class TestBuildFrozenSetCombineFn:
    def test_accumulates_and_deduplicates(self):
        fn = BuildFrozenSetCombineFn()
        acc = fn.create_accumulator()
        acc = fn.add_input(acc, "a.com")
        acc = fn.add_input(acc, "b.com")
        acc = fn.add_input(acc, "a.com")
        result = fn.extract_output(acc)
        assert result == frozenset({"a.com", "b.com"})

    def test_merge(self):
        fn = BuildFrozenSetCombineFn()
        a = {"a.com", "b.com"}
        b = {"b.com", "c.com"}
        merged = fn.merge_accumulators([a, b])
        assert fn.extract_output(merged) == frozenset({"a.com", "b.com", "c.com"})
