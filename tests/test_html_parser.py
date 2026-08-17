import pytest
from bs4 import BeautifulSoup

from html_parser import HTMLParser

VALID_HTML = """
<html>
<head>
    <title>Example Domain</title>
    <meta name="description" content="A page for testing">
    <meta name="keywords" content="test, parsing, html">
</head>
<body>
    <h1>Main heading</h1>
    <h2>Subheading</h2>
    <p>Some intro text with a <a href="page">relative link</a>.</p>
    <a href="/root">root link</a>
    <a href="https://other.com/external">external link</a>
    <a href="mailto:hi@example.com">email</a>
    <a>no href here</a>
    <ul>
        <li>first</li>
        <li>second</li>
    </ul>
    <img src="/img/logo.png" alt="Logo">
</body>
</html>
"""

BROKEN_HTML = "<html><body><p>hi<a href='/x'>broken"


@pytest.fixture
def parser() -> HTMLParser:
    return HTMLParser()


@pytest.mark.asyncio
async def test_parse_valid_html(parser: HTMLParser) -> None:
    result = await parser.parse_html(VALID_HTML, "https://example.com/")

    assert result["url"] == "https://example.com/"
    assert result["title"] == "Example Domain"
    assert result["metadata"]["description"] == "A page for testing"
    assert result["metadata"]["keywords"] == "test, parsing, html"

    assert len(result["text"]) > 0
    assert "intro text" in result["text"]

    headings = [(h["level"], h["text"]) for h in result["headings"]]
    assert ("h1", "Main heading") in headings
    assert ("h2", "Subheading") in headings

    assert result["images"] == [
        {"src": "https://example.com/img/logo.png", "alt": "Logo"}
    ]
    assert result["lists"] == [{"type": "ul", "items": ["first", "second"]}]


@pytest.mark.asyncio
async def test_parse_broken_html_does_not_crash(parser: HTMLParser) -> None:
    result = await parser.parse_html(BROKEN_HTML, "https://example.com/")

    assert isinstance(result, dict)
    for key in ("url", "title", "text", "links", "images", "headings", "lists"):
        assert key in result

    assert "https://example.com/x" in result["links"]


@pytest.mark.asyncio
async def test_parse_empty_html_returns_defaults(parser: HTMLParser) -> None:
    result = await parser.parse_html("", "https://example.com/")

    assert result["url"] == "https://example.com/"
    assert result["title"] == ""
    assert result["text"] == ""
    assert result["links"] == []
    assert result["images"] == []


def test_extract_links_filters_invalid(parser: HTMLParser) -> None:
    soup = BeautifulSoup(VALID_HTML, "lxml")

    links = parser.extract_links(soup, "https://example.com/")

    assert links == [
        "https://example.com/page",
        "https://example.com/root",
        "https://other.com/external",
    ]


def test_relative_links_become_absolute(parser: HTMLParser) -> None:
    soup = BeautifulSoup(VALID_HTML, "lxml")

    links = parser.extract_links(soup, "https://example.com/dir/")

    assert "https://example.com/dir/page" in links
    assert "https://example.com/root" in links
