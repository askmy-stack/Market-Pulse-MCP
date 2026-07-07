"""News text cleaning utilities."""

import re

HTML_TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")


def clean_text(text: str) -> str:
    text = HTML_TAG_RE.sub("", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def clean_headline(headline: str) -> str:
    return clean_text(headline)[:512]
