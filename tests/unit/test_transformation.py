from datetime import datetime

import pytest

from utils.transformation import (
    transform_gpu_records,
    transform_intelligence_records,
    transform_models,
    transform_news,
    transform_providers,
)


def test_transform_gpu_records():
    raw = {
        "snapshot": {
            "capturedAt": "2026-06-07T12:15:15.148Z",
            "offers": [
                {
                    "provider": "runpod",
                    "gpu_raw": "A100 PCIe",
                    "gpu_canonical": "A100-80GB",
                    "vram_gb": 80,
                    "on_demand_usd_hr": 1.19,
                    "spot_usd_hr": None,
                    "available_count": 2,
                    "region": None,
                    "source_url": "https://runpod.io",
                    "last_seen": "2026-06-07T12:15:15.765Z",
                },
                {
                    "provider": "lambda",
                    "gpu_raw": "H100",
                    "gpu_canonical": "H100",
                    "vram_gb": 80,
                    "on_demand_usd_hr": 2.49,
                    "spot_usd_hr": 1.79,
                    "available_count": 1,
                    "region": "us-east-1",
                    "source_url": "https://lambda.ai/pricing",
                    "last_seen": "2026-06-07T12:15:15.765Z",
                },
            ],
        }
    }

    records = transform_gpu_records(raw)

    assert len(records) == 2
    assert records[0]["gpu_canonical"] == "A100-80GB"
    assert records[0]["spot_usd_hr"] is None
    assert records[0]["region"] == "unknown"
    assert records[0]["captured_date"] == "2026-06-07T12:15:15.148Z"
    assert records[1]["spot_usd_hr"] == 1.79
    assert records[1]["region"] == "us-east-1"
    datetime.fromisoformat(records[0]["ingested_at"])


def test_transform_intelligence_records():
    raw = {
        "as_of": "2026-06-05T07:00:30.608Z",
        "models": [
            {
                "model_id": "gpt-5.5",
                "name": "GPT-5.5",
                "provider": "OpenAI",
                "tfii": 72.7,
                "rank": 1,
            }
        ],
    }

    records = transform_intelligence_records(raw)

    assert records == [
        {
            "model_id": "gpt-5.5",
            "name": "GPT-5.5",
            "provider": "OpenAI",
            "tfii": 72.7,
            "rank": 1,
            "captured_date": "2026-06-05T07:00:30.608Z",
            "ingested_at": records[0]["ingested_at"],
        }
    ]
    datetime.fromisoformat(records[0]["ingested_at"])


def test_transform_models():
    raw = {
        "providers": [
            {
                "id": "openai",
                "name": "OpenAI",
                "url": "https://openai.com",
                "models": [
                    {
                        "id": "gpt-4o",
                        "name": "GPT-4o",
                        "contextWindow": 128000,
                        "inputPrice": 5.0,
                        "outputPrice": 15.0,
                        "released": "2024-05-13",
                        "capabilities": ["text", "vision"],
                        "openSource": False,
                        "intelligence": {"tfii": 62.3},
                    }
                ],
            }
        ]
    }

    records = transform_models(raw)

    assert len(records) == 1
    assert records[0]["model_id"] == "gpt-4o"
    assert records[0]["provider_id"] == "openai"
    assert records[0]["capabilities"] == "text, vision"
    assert records[0]["tier"] == "unknown"
    assert records[0]["tfii_score"] == 62.3


def test_transform_providers():
    raw = {
        "providers": [
            {"id": "openai", "name": "OpenAI", "url": "https://openai.com"},
            {"id": "anthropic", "name": "Anthropic", "url": "https://anthropic.com"},
        ]
    }

    records = transform_providers(raw)

    assert [record["provider_id"] for record in records] == ["openai", "anthropic"]
    assert records[0]["name"] == "OpenAI"
    datetime.fromisoformat(records[0]["ingested_at"])


def test_transform_news_joins_categories():
    raw = {
        "articles": [
            {
                "id": "news-1",
                "title": "AI pricing update",
                "url": "https://example.com/news",
                "source": "Example",
                "sourceDomain": "example.com",
                "snippet": "Pricing changed.",
                "categories": ["Pricing", "Infrastructure"],
                "publishedAt": "2026-06-07T10:00:00.000Z",
                "fetchedAt": "2026-06-07T11:00:00.000Z",
            }
        ]
    }

    records = transform_news(raw)

    assert len(records) == 1
    assert records[0]["news_id"] == "news-1"
    assert records[0]["categories"] == "Pricing, Infrastructure"
    assert records[0]["source_domain"] == "example.com"
