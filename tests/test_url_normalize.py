"""Tests for URL normalization and domain extraction."""

from techsight_beam.utils.url_normalize import (
    extract_registered_domain,
    normalize_url,
)


class TestNormalizeUrl:
    def test_lowercases_hostname(self):
        assert normalize_url("https://Example.COM/Page") == "https://example.com/Page"

    def test_removes_trailing_slash(self):
        assert normalize_url("https://example.com/page/") == "https://example.com/page"

    def test_preserves_root_path(self):
        assert normalize_url("https://example.com/") == "https://example.com/"

    def test_removes_default_port_80(self):
        assert normalize_url("http://example.com:80/page") == "http://example.com/page"

    def test_removes_default_port_443(self):
        assert (
            normalize_url("https://example.com:443/page")
            == "https://example.com/page"
        )

    def test_preserves_non_default_port(self):
        assert (
            normalize_url("https://example.com:8080/page")
            == "https://example.com:8080/page"
        )

    def test_adds_scheme_when_missing(self):
        assert normalize_url("example.com/page") == "http://example.com/page"

    def test_strips_fragment(self):
        assert (
            normalize_url("https://example.com/page#section")
            == "https://example.com/page"
        )

    def test_preserves_query_string(self):
        assert (
            normalize_url("https://example.com/page?q=test")
            == "https://example.com/page?q=test"
        )

    def test_empty_string(self):
        assert normalize_url("") == ""

    def test_whitespace_stripped(self):
        assert normalize_url("  https://example.com  ") == "https://example.com/"

    def test_preserves_path_case(self):
        assert (
            normalize_url("https://example.com/CaseSensitive")
            == "https://example.com/CaseSensitive"
        )


class TestExtractRegisteredDomain:
    def test_simple_com(self):
        assert extract_registered_domain("https://www.example.com/page") == "example.com"

    def test_co_uk(self):
        assert extract_registered_domain("https://sub.example.co.uk/page") == "example.co.uk"

    def test_subdomain_stripped(self):
        assert extract_registered_domain("https://cdn.static.example.com") == "example.com"

    def test_bare_domain(self):
        assert extract_registered_domain("https://example.com") == "example.com"

    def test_lowercased(self):
        assert extract_registered_domain("https://WWW.EXAMPLE.COM") == "example.com"

    def test_no_suffix(self):
        assert extract_registered_domain("http://localhost/page") == "localhost"
