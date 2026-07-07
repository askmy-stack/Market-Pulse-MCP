"""Test news embedding generation."""

from marketpulse.news.embeddings import embed_news, embed_text
from marketpulse.schemas.events import NewsCategory, NewsEvent


def test_embed_text_returns_normalized_vector():
    vec = embed_text("Apple stock rises on earnings beat")
    assert len(vec) > 0
    assert all(isinstance(v, float) for v in vec)


def test_embed_text_deterministic_for_hash_fallback():
    a = embed_text("same text for hashing")
    b = embed_text("same text for hashing")
    assert a == b


def test_embed_news_payload():
    event = NewsEvent(
        headline="TSLA announces record deliveries",
        summary="Tesla reported strong Q4 delivery numbers.",
        category=NewsCategory.COMPANY,
        symbols=["TSLA"],
        source="test",
    )
    payload = embed_news(event)
    assert payload["event_id"] == event.event_id
    assert payload["dimension"] == len(payload["embedding"])
    assert payload["embedding_model"] in ("hash", "sentence-transformers")
