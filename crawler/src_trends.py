"""Google Trends - mức độ quan tâm TÌM KIẾM theo tỉnh (tín hiệu vùng miền chính).

Với mỗi nhóm 5 từ khoá (1 bài "mốc" + 4 bài), Google trả về cho từng tỉnh (63 tỉnh cũ, mã ISO VN-xx)
tỉ lệ % lượt tìm kiếm của mỗi bài trong nhóm. Vì bài mốc có mặt trong mọi nhóm, tỉ số
    share(bài) / share(mốc)
so sánh được giữa mọi bài trong cùng tỉnh -> xếp hạng bài thịnh hành của tỉnh đó.
Gộp 63 tỉnh cũ về 34 tỉnh mới (theo dân số) do batch layer làm.

Lưu ý khi dùng:
  - Từ khoá phải cùng kiểu "tên bài + nghệ sĩ" (tên bài trơn như "laviem" gom nhiều lượt tìm hơn -> không công bằng)
  - Google giới hạn tần suất (HTTP 429) -> mỗi nhóm cách nhau TRENDS_SLEEP_SEC giây, gặp 429 thì chờ lâu hơn rồi thử lại
  - Đây là endpoint web của trends.google.com (không chính thức); Google có API chính thức dạng thử nghiệm cần đăng ký
"""
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone

import requests

from common import TOPIC_TRENDS, UA

log = logging.getLogger("trends")
TRENDS = "https://trends.google.com/trends"
TOP_N = int(os.getenv("TRENDS_TOP_N", "40"))
SLEEP = float(os.getenv("TRENDS_SLEEP_SEC", "45"))
TIMEFRAME = os.getenv("TRENDS_TIMEFRAME", "now 7-d")
PROPERTY = os.getenv("TRENDS_PROPERTY", "youtube")        # "youtube" = tìm trên YouTube; "" = Google Search


class TrendsClient:
    def __init__(self):
        self.s = requests.Session()   # không dùng retry nhanh của http_session: 429 cần chờ lâu
        self.s.headers.update({"User-Agent": UA, "Accept-Language": "vi-VN,vi;q=0.9"})
        self._cookie()

    def _cookie(self):
        self.s.cookies.clear()
        self.s.get(f"{TRENDS}/?geo=VN", timeout=20)          # cookie NID bắt buộc, thiếu là bị 429 ngay

    def _json(self, url, params):
        for attempt in range(4):
            r = self.s.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = 60 * (attempt + 1) + random.uniform(0, 20)
                log.warning("Google Trends 429 -> chờ %.0f giây (lần %d)", wait, attempt + 1)
                time.sleep(wait)
                if attempt == 1:
                    self._cookie()
                continue
            r.raise_for_status()
            return json.loads(r.text[r.text.index("{"):])     # bỏ tiền tố chống XSSI ")]}'"
        raise RuntimeError("Google Trends trả 429 liên tục")

    def compare_by_region(self, keywords):
        req = {"comparisonItem": [{"keyword": k, "geo": "VN", "time": TIMEFRAME} for k in keywords],
               "category": 0, "property": PROPERTY}
        explore = self._json(f"{TRENDS}/api/explore", {"hl": "vi", "tz": "-420", "req": json.dumps(req)})
        widget = next(w for w in explore["widgets"] if w["id"] == "GEO_MAP")
        time.sleep(2 + random.uniform(0, 2))
        geo = self._json(f"{TRENDS}/api/widgetdata/comparedgeo",
                         {"hl": "vi", "tz": "-420", "req": json.dumps(widget["request"]), "token": widget["token"]})
        return geo["default"]["geoMapData"]


def _words(s, limit):
    s = re.sub(r"\([^)]*\)|\[[^\]]*\]|\"[^\"]*\"|“[^”]*”", " ", (s or "").lower())
    return [w for w in re.split(r"[^\w]+", s) if w][:limit]


def trends_query(title, artists):
    """Mọi từ khoá cùng một kiểu: 'tên bài (<= 6 từ) + nghệ sĩ chính (<= 2 từ)' để so sánh công bằng.
    'Tìm Em (feat. Bảo Anh)' + ['Hngle'] -> 'tìm em hngle'; nghệ sĩ 'Jack - J97' -> 'jack'."""
    from textnorm import clean_title

    t = _words(clean_title(title), 6)
    primary = (artists or [""])[0].split(" - ")[0]
    a = [w for w in _words(primary, 2) if w not in t]
    return " ".join(t + a)


def _candidates_from_es():
    es = os.getenv("ES_URL")
    if not es:
        return []
    r = requests.post(f"{es}/music-tracks/_search", timeout=15, json={
        "size": TOP_N * 3, "sort": [{"national_score": "desc"}], "query": {"range": {"national_score": {"gt": 0}}},
        "_source": ["track_key", "title", "artists"]})
    if r.status_code != 200:
        return []
    return [h["_source"] for h in r.json()["hits"]["hits"]]


def _candidates_from_charts(session, crawled_ms):
    """Chưa có batch view -> tự crawl BXH và chọn bài có điểm cao nhất (mỗi nền tảng lấy điểm tốt nhất)."""
    import src_apple
    import src_spotify
    import src_youtube
    import src_zing

    best, meta = {}, {}
    for fn in (src_zing.crawl_charts, src_spotify.crawl_charts, src_apple.crawl_charts, src_youtube.crawl_charts):
        try:
            for _, key, rec in fn(session, crawled_ms):
                if not key or rec.get("record_type") != "chart_entry":
                    continue
                pts = (rec["chart_size"] - rec["rank"] + 1) / rec["chart_size"]
                slot = (key.split("__")[0], rec["source"])
                best[slot] = max(best.get(slot, 0), pts)
                meta.setdefault(key.split("__")[0], {"track_key": key, "title": rec["title"], "artists": rec["artists"]})
        except Exception as e:  # noqa: BLE001
            log.warning("chart %s failed: %s", fn.__module__, e)
    score = {}
    for (title_slug, _), pts in best.items():
        score[title_slug] = score.get(title_slug, 0) + pts
    return [meta[t] for t, _ in sorted(score.items(), key=lambda x: -x[1])]


def candidates(session, crawled_ms):
    tracks = []
    try:
        tracks = _candidates_from_es()
    except Exception as e:  # noqa: BLE001
        log.info("ES chưa sẵn sàng (%s) -> lấy ứng viên từ BXH", e)
    if len(tracks) < 5:
        tracks = _candidates_from_charts(session, crawled_ms)
    out, seen = [], set()
    for t in tracks:
        q = trends_query(t["title"], t.get("artists"))
        slug = t["track_key"].split("__")[0]
        if q and q not in seen and slug not in seen:
            seen.update({q, slug})
            out.append(dict(t, query=q))
        if len(out) >= TOP_N:
            break
    return out


def crawl_trends(session, crawled_ms):
    cands = candidates(session, crawled_ms)
    if len(cands) < 2:
        raise RuntimeError("không có bài nào để tra Google Trends")
    client = TrendsClient()
    # Bài mốc phải có lượt tìm ở NHIỀU tỉnh nhất (bài đứng đầu BXH chưa chắc được tìm nhiều nhất).
    # Thử 10 bài đầu (2 lần tra); "hasData" của 1 từ khoá không phụ thuộc nhóm nên so được giữa 2 lần.
    coverage = {}
    for start in range(0, min(10, len(cands)), 5):
        probe = cands[start:start + 5]
        regions = client.compare_by_region([c["query"] for c in probe])
        for i, c in enumerate(probe):
            coverage[start + i] = sum(1 for r in regions if i < len(r.get("hasData") or []) and r["hasData"][i])
        time.sleep(SLEEP)
    best = max(coverage, key=lambda i: (coverage[i], -i))
    log.info("độ phủ (số tỉnh có dữ liệu): %s -> mốc '%s'",
             [(cands[i]["query"], v) for i, v in coverage.items()], cands[best]["query"])
    anchor = cands[best]
    rest = [c for i, c in enumerate(cands) if i != best]
    day = (datetime.fromtimestamp(crawled_ms / 1000, tz=timezone.utc) + timedelta(hours=7)).strftime("%Y-%m-%d")
    base = {"record_type": "trends_geo", "source": "google_trends", "snapshot_id": f"trends@{day}",
            "crawled_ms": crawled_ms, "timeframe": TIMEFRAME, "property": PROPERTY or "web",
            "anchor_key": anchor["track_key"], "anchor_query": anchor["query"]}
    groups = [rest[i:i + 4] for i in range(0, len(rest), 4)]
    log.info("Google Trends: %d bài, bài mốc '%s', %d nhóm, %.0f giây/nhóm", len(cands), anchor["query"], len(groups), SLEEP)
    failures = 0
    for gi, group in enumerate(groups):
        try:
            regions = client.compare_by_region([anchor["query"]] + [c["query"] for c in group])
            failures = 0
        except Exception as e:  # noqa: BLE001 - 1 nhóm lỗi thì bỏ qua nhóm đó
            failures += 1
            log.warning("nhóm %d lỗi: %s", gi, e)
            if failures >= 2:
                log.error("dừng sớm sau 2 nhóm lỗi liên tiếp")
                break
            continue
        members = ([(anchor, 0, True)] if gi == 0 else []) + [(c, i + 1, False) for i, c in enumerate(group)]
        for reg in regions:
            vals, has = reg.get("value") or [], reg.get("hasData") or []
            if not vals:
                continue
            for c, idx, is_anchor in members:
                rec = dict(base, group_id=gi, track_key=c["track_key"], title=c["title"], artists=c.get("artists") or [],
                           query=c["query"], geo_code=reg.get("geoCode"), geo_name=reg.get("geoName"),
                           share=int(vals[idx]), has_data=bool(has[idx]) if idx < len(has) else False,
                           anchor_share=int(vals[0]), anchor_has_data=bool(has[0]) if has else False,
                           is_anchor=is_anchor)
                yield TOPIC_TRENDS, c["track_key"], rec
        log.info("nhóm %d/%d xong: %s", gi + 1, len(groups), [c["query"] for c in group])
        if gi < len(groups) - 1:
            time.sleep(SLEEP + random.uniform(0, SLEEP / 3))
