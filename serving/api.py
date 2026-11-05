"""SERVING LAYER - REST API + dashboard web.

Đọc batch view và realtime view trong Elasticsearch rồi GỘP lại lúc truy vấn (đúng tinh thần
Lambda: query = f(batch view, realtime view)). Nhận lượt nghe từ giao diện web và đẩy vào Kafka
để vòng gợi ý realtime khép kín: bấm nghe -> Kafka -> Spark -> ES -> playlist mới.

Chạy cục bộ:  uvicorn api:app --port 8000   (cần ES_URL, KAFKA_BOOTSTRAP)
"""
import asyncio
import json
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import insights

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "shared"))
from provinces import PROVINCE_BY_CODE, PROVINCES  # noqa: E402
from textnorm import norm_text  # noqa: E402

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_EVENTS = os.getenv("TOPIC_EVENTS", "music.events")
STATIC_DIR = os.getenv("STATIC_DIR", os.path.join(HERE, "static"))
# trọng số gộp batch view + realtime view
W_BATCH, W_RT_PLAYS, W_RT_MENTIONS = (float(x) for x in os.getenv("MERGE_WEIGHTS", "0.6,0.3,0.1").split(","))

es = httpx.AsyncClient(base_url=ES_URL, timeout=15)


@asynccontextmanager
async def lifespan(_app):
    try:  # tạo index template (idempotent) trước khi phục vụ
        import es_setup
        await asyncio.to_thread(es_setup.ensure, ES_URL, 3)
    except Exception as e:  # noqa: BLE001
        print("es_setup skipped:", e, flush=True)
    yield
    await es.aclose()


app = FastAPI(title="VN Music Pulse API", version="1.0", lifespan=lifespan)
_producer = None
_cache = {}


# ------------------------------------------------------------------ helpers
async def search(index, body):
    r = await es.post(f"/{index}/_search", json=body)
    if r.status_code == 404:  # index chưa được tạo (pipeline chưa chạy tới)
        return {"hits": {"hits": [], "total": {"value": 0}}, "aggregations": {}}
    r.raise_for_status()
    return r.json()


async def get_doc(index, doc_id):
    r = await es.get(f"/{index}/_doc/{doc_id}")
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json().get("_source")


def sources(res):
    return [h["_source"] for h in res.get("hits", {}).get("hits", [])]


async def cached(key, ttl, fn):
    """Cache ngắn hạn trong RAM để API chịu tải cao (test HPA)."""
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    val = await fn()
    _cache[key] = (time.time(), val)
    return val


def now_ms():
    return int(time.time() * 1000)


def producer():
    global _producer
    if _producer is None:
        from kafka import KafkaProducer
        _producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP.split(","), acks="all", retries=5, linger_ms=5,
                                  key_serializer=lambda k: k.encode(),
                                  value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode())
    return _producer


# ------------------------------------------------------------------ hệ thống
@app.get("/api/health")
async def health():
    out = {"time": now_ms()}
    try:
        r = await es.get("/_cluster/health")
        out["elasticsearch"] = r.json().get("status")
    except Exception as e:  # noqa: BLE001
        out["elasticsearch"] = f"down: {e}"
    return out


@app.get("/api/meta")
async def meta():
    async def load():
        docs = await asyncio.gather(get_doc("music-meta", "batch_views"), get_doc("music-meta", "batch_similarity"),
                                    get_doc("music-an-kpis", "latest"))
        return {"batch_views": docs[0], "batch_similarity": docs[1], "kpis": docs[2]}
    return await cached("meta", 10, load)


# ------------------------------------------------------------------ tỉnh & xu hướng
async def realtime_by_province(minutes):
    since = now_ms() - minutes * 60_000
    plays, ments = await asyncio.gather(
        search("music-rt-plays", {"size": 0, "query": {"range": {"minute": {"gte": since}}},
                                  "aggs": {"p": {"terms": {"field": "province_code", "size": 40},
                                                 "aggs": {"plays": {"sum": {"field": "plays"}},
                                                          "likes": {"sum": {"field": "likes"}}}}}}),
        search("music-rt-mentions", {"size": 0, "query": {"range": {"published_ms": {"gte": now_ms() - 7 * 86400_000}}},
                                     "aggs": {"p": {"terms": {"field": "province_code", "size": 40},
                                                    "aggs": {"w": {"sum": {"field": "weight", "missing": 0.3}},
                                                             "authors": {"cardinality": {"field": "author_hash"}}}}}}))
    out = {}
    for b in plays.get("aggregations", {}).get("p", {}).get("buckets", []):
        out.setdefault(b["key"], {})["rt_plays"] = int(b["plays"]["value"])
        out[b["key"]]["rt_likes"] = int(b["likes"]["value"])
    for b in ments.get("aggregations", {}).get("p", {}).get("buckets", []):
        out.setdefault(b["key"], {})["mentions_7d"] = round(b["w"]["value"], 1)   # tự nhận ở tỉnh = 1, nhắc tên = 0.3
        out[b["key"]]["mention_authors_7d"] = b["authors"]["value"]
    return out


@app.get("/api/provinces")
async def provinces(minutes: int = 15):
    async def load():
        summary, rt = await asyncio.gather(search("music-an-province-summary", {"size": 100}),
                                           realtime_by_province(minutes))
        by = {s["province_code"]: s for s in sources(summary)}
        items = []
        for p in PROVINCES:
            s = by.get(p["code"], {})
            items.append({
                "code": p["code"], "name": p["name"], "region": p["region"], "subregion": p["subregion"],
                "lat": p["lat"], "lon": p["lon"], "population": p["population"],
                "top_title": s.get("top_title"), "top_artists": s.get("top_artists"), "top_genre": s.get("top_genre"),
                "fav_title": s.get("fav_title"), "fav_lift": s.get("fav_lift"),
                "mentions": s.get("mentions", 0), "plays_7d": s.get("plays_7d", 0),
                "listeners_7d": s.get("listeners_7d", 0),
                "confidence": s.get("confidence", 0), "confidence_label": s.get("confidence_label", "thấp"),
                "signals": s.get("signals", ["toàn quốc"]), "trend_tracks": s.get("trend_tracks", 0),
                "trend_coverage": s.get("trend_coverage", 0), "mention_authors": s.get("mention_authors", 0),
                "self_mentions": s.get("self_mentions", 0), **rt.get(p["code"], {}),
            })
        return {"window_minutes": minutes, "items": items}
    return await cached(f"prov:{minutes}", 5, load)


@app.get("/api/trending")
async def trending(province: str, limit: int = Query(20, le=50), mode: str = "merged", rt_minutes: int = 60):
    if province not in PROVINCE_BY_CODE:
        raise HTTPException(404, "unknown province")

    async def load():
        since = now_ms() - rt_minutes * 60_000
        batch, plays, ments, meta_doc = await asyncio.gather(
            search("music-trending-batch", {"size": 50, "query": {"term": {"province_code": province}},
                                            "sort": [{"rank": "asc"}]}),
            search("music-rt-plays", {"size": 0, "query": {"bool": {"filter": [
                {"term": {"province_code": province}}, {"range": {"minute": {"gte": since}}}]}},
                "aggs": {"t": {"terms": {"field": "track_key", "size": 50, "order": {"plays": "desc"}},
                               "aggs": {"plays": {"sum": {"field": "plays"}}, "likes": {"sum": {"field": "likes"}},
                                        "skips": {"sum": {"field": "skips"}},
                                        "meta": {"top_hits": {"size": 1, "_source": ["title", "artists", "genre"]}}}}}}),
            search("music-rt-mentions", {"size": 0, "query": {"bool": {"filter": [
                {"term": {"province_code": province}}, {"range": {"published_ms": {"gte": now_ms() - 7 * 86400_000}}}]}},
                "aggs": {"t": {"terms": {"field": "track_key", "size": 30},
                               "aggs": {"w": {"sum": {"field": "weight", "missing": 0.3}},
                                        "meta": {"top_hits": {"size": 1, "_source": ["title", "artists"]}}}}}}),
            get_doc("music-meta", "batch_views"))
        items = {}
        bdocs = sources(batch)
        bmax = max([d["score"] for d in bdocs] or [1]) or 1
        for d in bdocs:
            items[d["track_key"]] = {**d, "batch_rank": d["rank"], "batch": d["score"] / bmax, "rt_plays": 0, "rt_likes": 0,
                                     "rt_skips": 0, "rt_mentions": 0}
        pb = plays.get("aggregations", {}).get("t", {}).get("buckets", [])
        pmax = max([b["plays"]["value"] + 2 * b["likes"]["value"] for b in pb] or [1]) or 1
        for b in pb:
            it = items.setdefault(b["key"], {"track_key": b["key"], "batch": 0.0, "batch_rank": None, "rt_mentions": 0,
                                             **b["meta"]["hits"]["hits"][0]["_source"]})
            it.update(rt_plays=int(b["plays"]["value"]), rt_likes=int(b["likes"]["value"]),
                      rt_skips=int(b["skips"]["value"]),
                      rt_norm=(b["plays"]["value"] + 2 * b["likes"]["value"]) / pmax)
        mb = ments.get("aggregations", {}).get("t", {}).get("buckets", [])
        mmax = max([b["w"]["value"] for b in mb] or [1]) or 1
        for b in mb:
            it = items.setdefault(b["key"], {"track_key": b["key"], "batch": 0.0, "batch_rank": None, "rt_plays": 0,
                                             "rt_likes": 0, "rt_skips": 0,
                                             **b["meta"]["hits"]["hits"][0]["_source"]})
            it["rt_mentions"] = round(b["w"]["value"], 1)
            it["mention_norm"] = b["w"]["value"] / mmax
        for it in items.values():
            rt = it.get("rt_norm", 0.0)
            mn = it.get("mention_norm", 0.0)
            # tách điểm batch theo từng tín hiệu (c_nat + c_trd + c_cmt + c_evt = score của batch view)
            sc = it.get("score") or 0
            share = {k: (it.get(f"c_{k}") or 0) / sc if sc else 0 for k in ("nat", "trd", "cmt", "evt")}
            if mode == "batch":
                parts = {k: it["batch"] * v for k, v in share.items()}
            elif mode == "realtime":
                parts = {"nat": 0.0, "trd": 0.0, "cmt": 0.2 * mn, "evt": 0.8 * rt}
            else:
                b = W_BATCH * it["batch"]
                parts = {"nat": b * share["nat"], "trd": b * share["trd"],
                         "cmt": b * share["cmt"] + W_RT_MENTIONS * mn, "evt": b * share["evt"] + W_RT_PLAYS * rt}
            it["parts"] = {k: round(v, 5) for k, v in parts.items()}
            it["final"] = sum(parts.values())
        ranked = sorted(items.values(), key=lambda x: -x["final"])
        ranked = [x for x in ranked if x["final"] > 0][:limit]
        for i, x in enumerate(ranked, start=1):
            x["rank"] = i
            x["rank_change"] = (x["batch_rank"] - i) if x.get("batch_rank") else None
        p = PROVINCE_BY_CODE[province]
        conf = bdocs[0] if bdocs else {}
        return {"province": {"code": province, "name": p["name"], "region": p["region"],
                             "confidence": conf.get("confidence", 0), "confidence_label": conf.get("confidence_label", "thấp")},
                "mode": mode,
                "rt_minutes": rt_minutes, "batch_run": meta_doc, "items": ranked}
    return await cached(f"tr:{province}:{limit}:{mode}:{rt_minutes}", 3, load)


@app.get("/api/mentions")
async def mentions(province: str, limit: int = Query(12, le=50)):
    res = await search("music-rt-mentions", {"size": limit, "query": {"term": {"province_code": province}},
                                             "sort": [{"published_ms": "desc"}],
                                             "_source": ["title", "artists", "text", "like_count", "published_ms", "video_id",
                                                         "kind"]})
    return {"items": sources(res)}


# ------------------------------------------------------------------ BXH toàn quốc / nền tảng
@app.get("/api/national")
async def national(limit: int = Query(50, le=200)):
    async def load():
        res = await search("music-tracks", {"size": limit, "sort": [{"national_score": "desc"}],
                                            "query": {"range": {"national_score": {"gt": 0}}}})
        return {"items": sources(res)}
    return await cached(f"nat:{limit}", 10, load)


@app.get("/api/charts")
async def charts():
    res = await search("music-rt-charts", {"size": 0, "query": {"term": {"chart_scope": "VN"}}, "aggs": {
        "c": {"terms": {"field": "chart_id", "size": 50},
              "aggs": {"last": {"max": {"field": "crawled_ms"}}, "src": {"terms": {"field": "source", "size": 1}}}}}})
    return {"items": [{"chart_id": b["key"], "entries": b["doc_count"], "last_crawl_ms": b["last"]["value"],
                       "source": (b["src"]["buckets"] or [{"key": None}])[0]["key"]}
                      for b in res.get("aggregations", {}).get("c", {}).get("buckets", [])]}


@app.get("/api/charts/{chart_id}")
async def chart(chart_id: str, limit: int = Query(50, le=200)):
    res = await search("music-rt-charts", {"size": limit, "sort": [{"rank": "asc"}], "query": {"bool": {"filter": [
        {"term": {"chart_id": chart_id}}, {"term": {"chart_scope": "VN"}}]}}})
    return {"chart_id": chart_id, "items": sources(res)}


# ------------------------------------------------------------------ phân tích (batch views)
ANALYTICS = {"genre-province", "hourly", "platform-overlap", "artists", "rising", "chart-history", "province-summary",
             "signal-agreement"}


@app.get("/api/analytics/{name}")
async def analytics(name: str, province: Optional[str] = None, scope: Optional[str] = None,
                    chart_id: Optional[str] = None, size: int = Query(2000, le=5000)):
    if name not in ANALYTICS:
        raise HTTPException(404, f"available: {sorted(ANALYTICS)}")
    filters = [{"term": {k: v}} for k, v in (("province_code", province), ("scope", scope), ("chart_id", chart_id)) if v]

    async def load():
        res = await search(f"music-an-{name}", {"size": size, "query": {"bool": {"filter": filters}}})
        return {"name": name, "items": sources(res)}
    return await cached(f"an:{name}:{province}:{scope}:{chart_id}:{size}", 15, load)


@app.get("/api/realtime/timeline")
async def timeline(minutes: int = Query(60, le=1440)):
    async def load():
        res = await search("music-rt-plays", {"size": 0, "query": {"range": {"minute": {"gte": now_ms() - minutes * 60_000}}},
                                              "aggs": {"m": {"date_histogram": {"field": "minute", "fixed_interval": "1m"},
                                                             "aggs": {"plays": {"sum": {"field": "plays"}},
                                                                      "skips": {"sum": {"field": "skips"}},
                                                                      "likes": {"sum": {"field": "likes"}}}}}})
        return {"items": [{"minute": b["key"], "plays": int(b["plays"]["value"]), "skips": int(b["skips"]["value"]),
                           "likes": int(b["likes"]["value"])}
                          for b in res.get("aggregations", {}).get("m", {}).get("buckets", [])]}
    return await cached(f"tl:{minutes}", 3, load)


@app.get("/api/realtime/streaming")
async def streaming(minutes: int = Query(30, le=720)):
    """Throughput các streaming query của Spark (speed layer tự ghi mỗi 30 giây)."""
    latest, hist = await asyncio.gather(
        search("music-metrics", {"size": 20}),
        search("music-metrics-history", {"size": 2000, "sort": [{"ts_ms": "asc"}],
                                         "query": {"range": {"ts_ms": {"gte": now_ms() - minutes * 60_000}}}}))
    return {"latest": sources(latest), "history": sources(hist)}


# ------------------------------------------------------------------ gợi ý realtime
@app.get("/api/recommend/{user_id}")
async def recommend(user_id: str, province: Optional[str] = None):
    doc = await get_doc("music-recommendations", user_id)
    if doc:
        doc["age_sec"] = round((now_ms() - int(doc.get("updated_at", 0))) / 1000, 1)
        doc["cold_start"] = False
        return doc
    province = province or "ha-noi"
    tr = await trending(province=province, limit=20, mode="merged", rt_minutes=60)
    return {"user_id": user_id, "province_code": province, "cold_start": True, "based_on": [],
            "tracks": [{"rank": x["rank"], "track_key": x["track_key"], "title": x.get("title"),
                        "artists": x.get("artists"), "genre": x.get("genre"), "thumbnail": x.get("thumbnail"),
                        "score": round(x["final"], 4), "reason": f"Người nghe mới: đang thịnh hành ở {tr['province']['name']}"}
                       for x in tr["items"]]}


@app.get("/api/users")
async def users(province: Optional[str] = None, limit: int = Query(30, le=200)):
    q = {"term": {"province_code": province}} if province else {"match_all": {}}
    res = await search("music-recommendations", {"size": limit, "query": q, "sort": [{"updated_at": "desc"}],
                                                 "_source": ["user_id", "province_code", "updated_at", "events_30m"]})
    return {"items": sources(res)}


class Listen(BaseModel):
    user_id: str
    province_code: str
    track_key: str
    action: str = "play"


@app.post("/api/listen")
async def listen(ev: Listen):
    if ev.province_code not in PROVINCE_BY_CODE or ev.action not in ("play", "skip", "like"):
        raise HTTPException(400, "province_code/action không hợp lệ")
    try:  # thông tin bài chỉ để hiển thị; ES tạm lỗi vẫn nhận sự kiện
        track = await get_doc("music-tracks", ev.track_key) or {}
    except Exception:  # noqa: BLE001
        track = {}
    ts = now_ms()
    dur = int((track.get("duration_s") or 210) * 1000)
    event = {"event_id": str(uuid.uuid4()), "user_id": ev.user_id, "province_code": ev.province_code,
             "track_key": ev.track_key, "title": track.get("title", ev.track_key), "artists": track.get("artists", []),
             "genre": track.get("genre", "Khác"), "duration_ms": dur, "source": "web", "action": ev.action,
             "listen_ms": dur if ev.action == "play" else (8000 if ev.action == "skip" else 0), "ts_ms": ts}
    if os.getenv('INSIGHT_BACKEND', 'archive') == 'archive':
        # Actual button clicks in local mode, explicitly separate from Kafka delivery.
        records, _ = await asyncio.to_thread(insights.load_local)
        known = next((r for r in records if (r.get('track_key') or insights.identity(r)) == ev.track_key
                      and r.get('record_type') == 'chart_entry'), None)
        if not known or not ev.user_id.strip() or len(ev.user_id) > 100:
            raise HTTPException(400, 'Bài hát hoặc mã người dùng không hợp lệ')
        event.update(title=known.get('title'), artists=known.get('artists'), genre=known.get('genre_hint') or 'unknown',
                     listen_ms=None, duration_ms=None)
        def append_event():
            path = insights.ROOT / 'out/insight-web-events.jsonl'
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('a', encoding='utf-8') as f:
                f.write(json.dumps(event, ensure_ascii=False) + '\n')
        await asyncio.to_thread(append_event)
        return {'event': event, 'mode': 'archive', 'produced_at': now_ms()}
    try:
        meta = await asyncio.to_thread(lambda: producer().send(TOPIC_EVENTS, key=ev.user_id, value=event).get(timeout=10))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(503, f"Kafka không sẵn sàng: {e}")
    return {"event": event, "kafka": {"topic": meta.topic, "partition": meta.partition, "offset": meta.offset},
            "produced_at": now_ms()}


@app.get("/api/search")
async def search_tracks(q: str, limit: int = Query(20, le=50)):
    nq = norm_text(q)
    if not nq:
        return {"items": []}
    res = await search("music-tracks", {"size": limit, "sort": [{"national_score": "desc"}],
                                        "query": {"wildcard": {"search_text": {"value": f"*{nq}*"}}},
                                        "_source": ["track_key", "title", "artists", "genre", "thumbnail",
                                                    "national_score", "sources"]})
    return {"items": sources(res)}


@app.get("/api/similar/{track_key}")
async def similar(track_key: str):
    return await get_doc("music-item-sim", track_key) or {"track_key": track_key, "neighbors": []}


# ------------------------------------------------------------------ dashboard tĩnh
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "insights.html"))


@app.get("/api/insights/{feature}")
async def insight_api(feature: str, days: int = Query(30, ge=1, le=365), as_of: Optional[str] = None,
                      countries: str = 'VN,TH', chart_date: Optional[str] = None,
                      top_n: int = Query(50, ge=1, le=200), source: str = 'web',
                      group_by: str = 'track', track: Optional[str] = None, genre: Optional[str] = None):
    if feature not in ('lifecycle', 'playlists', 'countries', 'feedback') or source not in ('web', 'simulator', 'unknown') or group_by not in ('track', 'genre', 'province', 'hour'):
        raise HTTPException(400, 'Bộ lọc không hợp lệ')
    cs = list(dict.fromkeys(c.strip().upper() for c in countries.split(',') if c.strip()))
    if not 2 <= len(cs) <= 10 or any(len(c) != 2 or not c.isalpha() for c in cs):
        raise HTTPException(400, 'Chọn từ 2 đến 10 mã quốc gia ISO')
    try:
        for d in (as_of, chart_date):
            if d:
                from datetime import date
                date.fromisoformat(d)
    except ValueError:
        raise HTTPException(400, 'Ngày phải theo YYYY-MM-DD')
    mode = os.getenv('INSIGHT_BACKEND', 'archive')
    if mode == 'elasticsearch':
        records = []
        scroll = None
        try:
            response = await es.post('/music-insight-raw/_search', params={'scroll': '1m'},
                                     json={'size': 1000, 'sort': ['_doc'], 'query': {'match_all': {}}})
            response.raise_for_status()
            page = response.json()
            while True:
                scroll = page.get('_scroll_id')
                hits = page['hits']['hits']
                if not hits:
                    break
                records.extend(insights.normalize(json.loads(h['_source']['value'])) for h in hits)
                if len(records) > 200000:
                    raise HTTPException(422, 'Vượt giới hạn 200.000 bản ghi; cần batch view cho tập dữ liệu này.')
                response = await es.post('/_search/scroll', json={'scroll': '1m', 'scroll_id': scroll})
                response.raise_for_status()
                page = response.json()
        except httpx.HTTPError:
            raise HTTPException(503, 'Chưa đọc được music-insight-raw; kiểm tra streaming/import dữ liệu.')
        finally:
            if scroll:
                try:
                    await es.request('DELETE', '/_search/scroll', json={'scroll_id': [scroll]})
                except httpx.HTTPError:
                    pass
        metadata = {'mode': mode, 'note': 'Dữ liệu raw từ pipeline/import; không dùng archive thay thế khi ES lỗi.'}
    else:
        records, metadata = await asyncio.to_thread(insights.load_local)
    try:
        result = await asyncio.to_thread(insights.analyze, records, feature, days, as_of,
                                        countries=cs, chart_date=chart_date, top_n=top_n, source=source,
                                        group_by=group_by, track=track, genre=genre)
    except (ValueError, TypeError, KeyError) as exc:
        raise HTTPException(422, f'Dữ liệu không đúng schema: {type(exc).__name__}')
    result['data'] = metadata
    return result
