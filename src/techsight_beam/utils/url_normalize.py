"""URL normalization and domain extraction utilities."""

from urllib.parse import urlparse, urlunparse

import tldextract

# Use the bundled Public Suffix List snapshot — no network calls on workers.
_extractor = tldextract.TLDExtract(suffix_list_urls=())


def normalize_url(url: str) -> str:
    """Normalize a URL for consistent matching.

    - Lowercases scheme and hostname
    - Removes default ports (80 for http, 443 for https)
    - Removes trailing slashes from paths (preserves root '/')
    - Strips fragments
    - Preserves query strings and path case
    """
    url = url.strip()
    if not url:
        return url

    if not url.startswith(("http://", "https://", "//")):
        url = "http://" + url

    try:
        parsed = urlparse(url)
    except Exception:
        return url.lower()

    scheme = (parsed.scheme or "http").lower()
    hostname = (parsed.hostname or "").lower()
    if hostname.startswith("www."):
        hostname = hostname[4:]

    port = parsed.port
    if port in (80, 443, None):
        netloc = hostname
    else:
        netloc = f"{hostname}:{port}"

    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    return urlunparse((scheme, netloc, path, parsed.params, parsed.query, ""))


def extract_registered_domain(normalized_url: str) -> str:
    """Extract the registered domain from a URL.

    Uses the Public Suffix List to correctly handle multi-part TLDs like
    ``.co.uk`` and ``.com.au``.

    Examples::

        https://sub.example.co.uk/page  →  example.co.uk
        https://www.example.com/page    →  example.com
    """
    ext = _extractor(normalized_url)
    if ext.suffix:
        return f"{ext.domain}.{ext.suffix}".lower()
    return ext.domain.lower()
