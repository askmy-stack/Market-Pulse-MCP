"""Lexicon-based sentiment analysis."""

from __future__ import annotations

POSITIVE = {"beat", "upgrade", "growth", "rally", "jump", "strong", "boost", "gain", "surge", "profit"}
NEGATIVE = {"miss", "downgrade", "scrutiny", "slip", "fall", "weak", "loss", "decline", "cut", "risk"}


def analyze_sentiment(text: str) -> float:
    words = {w.strip(".,!?").lower() for w in text.split()}
    pos = len(words & POSITIVE)
    neg = len(words & NEGATIVE)
    total = pos + neg
    if total == 0:
        return 0.0
    return (pos - neg) / total
