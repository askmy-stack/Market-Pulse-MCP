"""Lightweight news embedding generation."""

from __future__ import annotations

import hashlib
import math
from typing import Any

from marketpulse.observability.logging import get_logger
from marketpulse.schemas.events import NewsEvent

logger = get_logger(__name__)

_EMBEDDER = None
_EMBEDDER_TYPE = "hash"


def _hash_embedding(text: str, dim: int = 64) -> list[float]:
    """Deterministic hash-based embedding fallback."""
    vec = [0.0] * dim
    tokens = text.lower().split()
    for token in tokens:
        digest = hashlib.sha256(token.encode()).digest()
        for i in range(dim):
            vec[i] += (digest[i % len(digest)] / 255.0) - 0.5
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [round(v / norm, 6) for v in vec]


def _try_sentence_transformer():
    global _EMBEDDER, _EMBEDDER_TYPE
    try:
        from sentence_transformers import SentenceTransformer

        _EMBEDDER = SentenceTransformer("all-MiniLM-L6-v2")
        _EMBEDDER_TYPE = "sentence-transformers"
        logger.info("embeddings_using_sentence_transformers")
    except ImportError:
        _EMBEDDER = None
        _EMBEDDER_TYPE = "hash"
        logger.info("embeddings_using_hash_fallback")


def embed_text(text: str) -> list[float]:
    """Return embedding vector for text."""
    global _EMBEDDER
    if _EMBEDDER is None and _EMBEDDER_TYPE == "hash":
        _try_sentence_transformer()

    if _EMBEDDER is not None:
        try:
            vector = _EMBEDDER.encode(text, normalize_embeddings=True)
            return [float(x) for x in vector.tolist()]
        except Exception as exc:
            logger.warning("sentence_transformer_failed", error=str(exc))

    return _hash_embedding(text)


def embed_news(event: NewsEvent) -> dict[str, Any]:
    """Build embedding payload for a news event."""
    text = f"{event.headline}. {event.summary}"
    vector = embed_text(text)
    return {
        "event_id": event.event_id,
        "headline": event.headline,
        "symbols": event.symbols,
        "category": event.category.value,
        "embedding": vector,
        "embedding_model": _EMBEDDER_TYPE,
        "dimension": len(vector),
    }
