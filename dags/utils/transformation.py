from datetime import datetime, timezone

def transform_providers(raw: dict) -> list[dict]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    result = []
    for p in raw.get("providers", []):
        result.append({
            "provider_id": p["id"],
            "name":        p["name"],
            "url":         p["url"],
            "ingested_at": ingested_at,
        })
    return result


def transform_models(raw: dict) -> list[dict]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    captured_date = datetime.now(timezone.utc).date().isoformat()
    result = []
    for p in raw.get("providers", []):
        for m in p.get("models", []):
            intelligence = m.get("intelligence") or {}
            result.append({
                "model_id":       m["id"],
                "provider_id":    p["id"],
                "model_name":     m["name"],
                "context_window": m.get("contextWindow"),
                "input_price":    m.get("inputPrice"),
                "output_price":   m.get("outputPrice"),
                "released":       m.get("released"),
                "capabilities":   ", ".join(m.get("capabilities", [])),
                "open_source":    m.get("openSource", False),
                "tier":           m.get("tier") or "unknown",
                "tfii_score":     intelligence.get("tfii"),
                "captured_date":  captured_date,
                "ingested_at":    ingested_at,
            })
    return result

def transform_news(raw: dict | list[dict]) -> list[dict]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    articles = raw.get("articles", [])
    result = []
    for n in articles:
        result.append({
            "news_id": n["id"],
            "title":   n["title"],
            "url":     n["url"],
            "source":  n["source"],
            "source_domain": n["sourceDomain"],
            "snippet": n["snippet"],
            "categories": ", ".join(n.get("categories", [])),
            "published_at": n.get("publishedAt"),
            "fetched_at": n.get("fetchedAt"),
            "ingested_at": ingested_at,
        })
    return result

def transform_intelligence_records(raw: dict) -> list[dict]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    captured_date = raw.get("as_of")
    result = []
    for r in raw.get("models", []):
        result.append({
            "model_id": r["model_id"],
            "name": r["name"],
            "provider": r["provider"],
            "tfii": r.get("tfii"),
            "rank": r.get("rank"),
            "captured_date": captured_date,
            "ingested_at": ingested_at,
        })
    return result

def transform_gpu_records(raw: dict) -> list[dict]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    snapshot = raw.get("snapshot", {})
    captured_date = snapshot.get("capturedAt")
    result = []
    for r in snapshot.get("offers", []):
        result.append({
            "provider":         r["provider"],
            "gpu_raw":          r["gpu_raw"],
            "gpu_canonical":    r["gpu_canonical"],
            "vram_gb":          r["vram_gb"],
            "on_demand_usd_hr": r["on_demand_usd_hr"],
            "spot_usd_hr":      r["spot_usd_hr"],
            "available_count":  r["available_count"],
            "region":           r["region"] or "unknown",
            "source_url":       r["source_url"],
            "last_seen":        r["last_seen"],
            "captured_date":    captured_date,
            "ingested_at":      ingested_at,
        })
    return result
