"""Probe public country charts and preserve raw responses and comparison evidence.

This diagnostic does not publish to Kafka or modify production country settings.
Uses Python's standard library; no API keys or additional packages are needed.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import html
from itertools import combinations
import json
from pathlib import Path
import re
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/128.0 Safari/537.36"
SOURCES = ("apple_music", "spotify_kworb", "youtube")


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def plain(value):
    return html.unescape(re.sub(r"<[^>]+>", "", value)).strip()


def number(value):
    value = re.sub(r"[^0-9-]", "", plain(value or ""))
    return int(value) if value not in ("", "-") else None


def request(url, body=None):
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.8"}
    payload = None
    if body is not None:
        headers.update({"Content-Type": "application/json", "Origin": "https://charts.youtube.com"})
        payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=25) as response:
            return response.status, response.read(), response.geturl()
    except urllib.error.HTTPError as error:
        return error.code, error.read(), url


def apple(country, data):
    feed = data["feed"]
    if feed.get("country", country).lower() != country:
        raise ValueError("Returned Apple storefront differs from requested country")
    rows = []
    for rank, item in enumerate(feed["results"], 1):
        rows.append({"platform_id": item.get("id"), "title": item.get("name"),
                     "artists": [item["artistName"]] if item.get("artistName") else [],
                     "rank": rank, "previous_rank": None, "metric_name": None, "metric_value": None,
                     "url": item.get("url"), "release_date": item.get("releaseDate"),
                     "genres": [g.get("name") for g in item.get("genres", []) if g.get("name") != "Music"]})
    return rows, {"source_updated_at": feed.get("updated"), "source_period": None,
                  "period_type": "latest_feed", "reported_country": feed.get("country")}


def kworb(country, page):
    headings = " ".join(plain(x) for x in re.findall(r'<span class="pagetitle">(.*?)</span>', page, re.S))
    date_match = re.search(r"\b(20\d{2}/\d{2}/\d{2})\b", headings)
    rows = []
    for fragment in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.S):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", fragment, re.S)
        match = re.search(r'href="\.\./track/([A-Za-z0-9]+)\.html">(.*?)</a>', fragment, re.S)
        if len(cells) < 11 or not match:
            continue
        rank = number(cells[0])
        movement = plain(cells[1])
        previous = rank + int(movement) if re.fullmatch(r"[+-]\d+", movement) else rank if movement == "=" else None
        artists = re.findall(r'href="\.\./artist/[^"]+">([^<]+)</a>', fragment)
        rows.append({"platform_id": match[1], "title": html.unescape(match[2]),
                     "artists": [html.unescape(a) for a in artists], "rank": rank, "previous_rank": previous,
                     "metric_name": "daily_streams", "metric_value": number(cells[6]),
                     "total_plays": number(cells[10]), "url": "https://open.spotify.com/track/" + match[1]})
    return rows, {"source_updated_at": None,
                  "source_period": date_match[1].replace("/", "-") if date_match else None,
                  "period_type": "daily", "source_heading": headings}


def youtube(country, data):
    content = data["contents"]["sectionListRenderer"]["contents"][0]["musicAnalyticsSectionRenderer"]["content"]
    items = content["trackTypes"][0].get("trackViews", [])
    rows = []
    for item in items:
        metadata = item.get("chartEntryMetadata", {})
        vid = item.get("encryptedVideoId")
        rows.append({"platform_id": vid, "title": item.get("name"),
                     "artists": [a["name"] for a in item.get("artists", [])],
                     "rank": metadata.get("currentPosition"), "previous_rank": metadata.get("previousPosition"),
                     "metric_name": "weekly_views", "metric_value": number(str(item.get("viewCount", ""))),
                     "url": "https://www.youtube.com/watch?v=" + vid if vid else None})
    return rows, {"source_updated_at": None, "source_period": None, "period_type": "weekly",
                  "period_note": "Exact native week not extracted; retain raw response. Do not align to crawl date."}


def probe(source, country, directory, cached_result=None):
    start = cached_result["requested_at"] if cached_result else now()
    body = None
    if source == "apple_music":
        url = f"https://rss.marketingtools.apple.com/api/v2/{country}/music/most-played/100/songs.json"
    elif source == "spotify_kworb":
        url = f"https://kworb.net/spotify/country/{country}_daily.html"
    else:
        url = "https://charts.youtube.com/youtubei/v1/browse?alt=json"
        body = {"context": {"client": {"clientName": "WEB_MUSIC_ANALYTICS", "clientVersion": "2.0",
                                       "hl": "en", "gl": country.upper(), "theme": "MUSIC"}},
                "browseId": "FEmusic_analytics_charts_home",
                "query": f"perspective=CHART_DETAILS&chart_params_country_code={country}&chart_params_chart_type=TRACKS&chart_params_period_type=WEEKLY"}
    result = {"source": source, "country_code": country.upper(), "requested_at": start,
              "source_url": url, "request_body": body, "status": "failed", "records": 0}
    rows = []
    name = source + "_" + country
    try:
        if cached_result:
            if not cached_result.get("raw_file"):
                return cached_result, []
            url = cached_result["source_url"]
            result["source_url"] = url
            status, final_url = cached_result["http_status"], cached_result["final_url"]
            raw_path = directory / cached_result["raw_file"]
            raw = raw_path.read_bytes()
        else:
            status, raw, final_url = request(url, body)
            suffix = ".html" if source == "spotify_kworb" else ".json"
            raw_path = directory / "http" / (name + suffix)
            raw_path.write_bytes(raw)
        result.update({"http_status": status, "final_url": final_url,
                       "raw_file": str(raw_path.relative_to(directory)).replace("\\", "/")})
        if status != 200:
            raise ValueError(f"HTTP {status}")
        text = raw.decode("utf-8")
        parser = {"apple_music": apple, "spotify_kworb": kworb, "youtube": youtube}[source]
        rows, period = parser(country, text if source == "spotify_kworb" else json.loads(text))
        if not rows:
            raise ValueError("HTTP 200 returned no usable chart entries")
        for row in rows:
            row.update({"source": "spotify" if source == "spotify_kworb" else source,
                        "data_provider": source, "country_code": country.upper(), "chart_scope": country.upper(),
                        "chart_id": name, "crawled_at": start, "source_url": url, **period})
        issues = []
        for key in ("platform_id", "title", "artists", "rank"):
            if any(not row.get(key) for row in rows):
                issues.append("missing " + key)
        if len({r["rank"] for r in rows}) != len(rows):
            issues.append("duplicate ranks")
        if len({r["platform_id"] for r in rows}) != len(rows):
            issues.append("duplicate platform IDs")
        if any(not isinstance(r["rank"], int) or r["rank"] < 1 for r in rows):
            issues.append("invalid ranks")
        elif {r["rank"] for r in rows} != set(range(1, max(r["rank"] for r in rows) + 1)):
            issues.append("gaps in ranks")
        if source != "apple_music" and any(r.get("metric_value") is None or r["metric_value"] < 0 for r in rows):
            issues.append("missing or negative metric")
        output = directory / "records" / (name + ".jsonl")
        with output.open("w", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")
        result.update({"status": "partial" if issues else "success", "records": len(rows), "issues": issues,
                       "data_file": str(output.relative_to(directory)).replace("\\", "/"), **period,
                       "fields_present": {key: sum(bool(row.get(key)) for row in rows)
                                          for key in ("platform_id", "title", "artists", "rank", "previous_rank", "metric_value", "total_plays")},
                       "sample": [{key: row.get(key) for key in ("rank", "platform_id", "title", "artists", "metric_value")} for row in rows[:3]]})
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
    return result, rows


def compare(probes, records):
    comparisons = []
    for source in SOURCES:
        samples = [(p, rows) for p, rows in zip(probes, records) if p["source"] == source and p["status"] == "success"]
        for (a, ra), (b, rb) in combinations(samples, 2):
            top_a = {r["platform_id"] for r in ra if r["rank"] <= 50}
            top_b = {r["platform_id"] for r in rb if r["rank"] <= 50}
            common = top_a & top_b
            same_period = bool(a.get("source_period") and a.get("source_period") == b.get("source_period"))
            comparisons.append({"source": source, "country_a": a["country_code"], "country_b": b["country_code"],
                                "common_top50": len(common), "jaccard_top50": round(len(common) / len(top_a | top_b), 4),
                                "verified_same_native_period": same_period,
                                "identity_method": "exact platform ID; Apple/YouTube IDs may undercount alternate editions/videos"})
    # A concrete, same-date Spotify track present in multiple sampled countries.
    groups = {}
    for probe_result, rows in zip(probes, records):
        if probe_result["source"] == "spotify_kworb" and probe_result["status"] == "success" and probe_result.get("source_period"):
            for row in rows:
                groups.setdefault((row["source_period"], row["platform_id"]), []).append(row)
    shared = sorted((r for r in groups.values() if len(r) >= 2), key=lambda r: (-len(r), min(x["rank"] for x in r)))
    shared_samples = [{"platform_id": rows[0]["platform_id"], "title": rows[0]["title"],
                       "artists": rows[0]["artists"], "source_period": rows[0]["source_period"],
                       "countries": [{"country_code": r["country_code"], "rank": r["rank"], "daily_streams": r["metric_value"]} for r in rows]}
                      for rows in shared[:5]]
    return comparisons, shared_samples


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--countries", default="vn,th,id,us,jp")
    ap.add_argument("--sources", default=",".join(SOURCES))
    ap.add_argument("--output-dir")
    ap.add_argument("--reparse", action="store_true", help="Reparse existing raw evidence without making network requests")
    args = ap.parse_args()
    countries = list(dict.fromkeys(c.strip().lower() for c in args.countries.split(",")))
    sources = list(dict.fromkeys(s.strip() for s in args.sources.split(",")))
    if any(not re.fullmatch(r"[a-z]{2}", c) for c in countries) or any(s not in SOURCES for s in sources):
        ap.error("Use two-letter country codes and sources: " + ",".join(SOURCES))
    directory = Path(args.output_dir) if args.output_dir else ROOT / "out" / ("country-feasibility-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    directory.mkdir(parents=True, exist_ok=True)
    for sub in ("http", "records"):
        (directory / sub).mkdir(exist_ok=True)
    if not args.reparse and (any((directory / "http").iterdir()) or any((directory / "records").iterdir())):
        ap.error("Choose a new output directory to preserve previous evidence")
    started = now()
    pairs = []
    previous = None
    if args.reparse:
        previous = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
        countries = [c.lower() for c in previous["countries"]]
        pairs = [probe(p["source"], p["country_code"].lower(), directory, cached_result=p) for p in previous["probes"]]
        started = previous["started_at"]
    else:
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(probe, source, country, directory) for source in sources for country in countries]
            for future in as_completed(futures):
                result, rows = future.result()
                pairs.append((result, rows))
                print(json.dumps({key: result.get(key) for key in ("source", "country_code", "http_status", "status", "records", "source_period", "error")}))
    pairs.sort(key=lambda pair: (pair[0]["source"], countries.index(pair[0]["country_code"].lower())))
    probes, records = map(list, zip(*pairs))
    comparisons, samples = compare(probes, records)
    summary = {"started_at": started, "completed_at": previous["completed_at"] if previous else now(), "countries": [c.upper() for c in countries],
               "scope": "One live public chart per source-country. Record count is not unique song count.",
               "total_records": sum(p["records"] for p in probes), "probes": probes,
               "comparisons": comparisons, "shared_spotify_samples": samples}
    if previous:
        summary["reparsed_at"] = now()
    (directory / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output_dir": str(directory), "total_records": summary["total_records"],
                      "successful_probes": sum(p["status"] == "success" for p in probes), "probes": len(probes)}))


if __name__ == "__main__":
    main()
