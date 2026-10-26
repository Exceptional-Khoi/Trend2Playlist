"""Capture additional public country chart sources, preserving HTTP evidence.

No login or API keys. Does not change the production Kafka/Spark pipeline.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import http.cookiejar
import json
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/128.0 Safari/537.36"


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def capture(key, source, country, url, directory, opener=None):
    result = {"key": key, "source": source, "country_code": country, "source_url": url,
              "requested_at": timestamp(), "status": "failed"}
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.8"})
    try:
        try:
            response = (opener.open if opener else urllib.request.urlopen)(req, timeout=25)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read()
            status = response.code
            content_type = response.headers.get("Content-Type", "")
            result.update({"http_status": status, "final_url": response.geturl(),
                           "content_type": content_type, "bytes": len(raw)})
            suffix = ".json" if "json" in content_type else ".html"
            path = directory / "http" / (key + suffix)
            path.write_bytes(raw)
            result["raw_file"] = path.relative_to(directory).as_posix()
            result["status"] = "success" if status == 200 else "failed"
            if status != 200:
                result["error"] = f"HTTP {status}"
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
    print(json.dumps({k: result.get(k) for k in ("key", "http_status", "status", "bytes", "error")}), flush=True)
    return result


def targets():
    result = []
    for code, name in (("VN", "vietnam"), ("TH", "thailand"), ("ID", "indonesia"), ("US", "united-states"), ("JP", "japan")):
        result.append(("shazam_" + code.lower(), "shazam", code, "https://www.shazam.com/charts/top-200/" + name))
    result.append(("deezer_owner", "deezer_discovery", None, "https://api.deezer.com/user/637006841/playlists?limit=1000"))
    for code, pid in (("US", "1313621735"), ("GB", "1111142221"), ("BR", "1111141961")):
        result.append(("deezer_" + code.lower(), "deezer", code, "https://api.deezer.com/playlist/" + pid))
    for code in ("TW", "HK", "SG", "JP"):
        result.append(("kkbox_" + code.lower(), "kkbox", code,
                       "https://kma.kkbox.com/charts/daily/song?terr=" + code.lower() + "&lang=tc"))
    result.extend([
        ("sea_weekly", "official_sea", None, "https://www.officialseacharts.com/weeklychart"),
        ("rias_sg", "official_sea_rias", "SG", "https://www.rias.org.sg/the-official-singapore-charts/"),
    ])
    return result


def followup_targets(directory):
    """Public HTML fallbacks and URLs discovered in the captured source pages."""
    result = []
    for code, pid in (("US", "1313621735"), ("GB", "1111142221"), ("BR", "1111141961")):
        result.append(("deezer_html_" + code.lower(), "deezer", code,
                       "https://www.deezer.com/en/playlist/" + pid))
    result.append(("joox_id", "joox", "ID", "https://www.joox.com/id/chart/36"))
    path = directory / "http" / "rias_sg.html"
    if path.exists():
        source = path.read_text(encoding="utf-8")
        for match in re.finditer(r"window\['ninja_table_instance_\d+'\]\s*=\s*", source):
            config, _ = json.JSONDecoder().raw_decode(source, match.end())
            url = config.get("init_config", {}).get("data_request_url")
            if url and "table_id=3919" in url:
                result.append(("rias_sg_main", "official_sea_rias", "SG", url))
    return result


def trends(directory):
    """One identical worldwide comparison; do not compare separately scaled countries."""
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    records = []
    base_url = "https://trends.google.com"
    records.append(capture("trends_cookie", "google_trends_session", None, base_url + "/trends/?geo=VN", directory, opener))
    keywords = ["Seven Jung Kook", "hate that i made you love me Ariana Grande", "Earrings Malcolm Todd"]
    query = {"comparisonItem": [{"keyword": k, "geo": "", "time": "now 7-d"} for k in keywords],
             "category": 0, "property": "youtube"}
    explore_url = base_url + "/trends/api/explore?" + urllib.parse.urlencode({"hl": "en", "tz": "-420", "req": json.dumps(query)})
    result = capture("trends_explore", "google_trends", None, explore_url, directory, opener)
    result["keywords"] = keywords
    records.append(result)
    if result["status"] != "success":
        return records
    try:
        text = (directory / result["raw_file"]).read_text(encoding="utf-8")
        document = json.loads(text[text.index("{"):])
        widget = next(w for w in document["widgets"] if w["id"] == "GEO_MAP")
        url = base_url + "/trends/api/widgetdata/comparedgeo?" + urllib.parse.urlencode(
            {"hl": "en", "tz": "-420", "req": json.dumps(widget["request"]), "token": widget["token"]})
        geo = capture("trends_geo", "google_trends", None, url, directory, opener)
        geo.update({"keywords": keywords, "timeframe": "now 7-d", "property": "youtube"})
        records.append(geo)
    except Exception as error:
        result["parse_error"] = f"{type(error).__name__}: {error}"
    return records


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output-dir")
    ap.add_argument("--followup", action="store_true", help="Append discovered URLs and public HTML fallbacks to an existing capture")
    ap.add_argument("--reparse", action="store_true", help="Normalize saved responses without network access")
    args = ap.parse_args()
    if args.reparse:
        if not args.output_dir:
            ap.error("--reparse requires --output-dir")
        from country_additional_normalizer import normalize
        normalize(Path(args.output_dir))
        return
    directory = Path(args.output_dir) if args.output_dir else ROOT / "out" / ("country-additional-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "http").mkdir(exist_ok=True)
    if any((directory / "http").iterdir()) and not args.followup:
        ap.error("Choose a new output directory to preserve evidence")
    if args.followup:
        if not args.output_dir:
            ap.error("--followup requires --output-dir")
        summary = json.loads((directory / "fetch-summary.json").read_text(encoding="utf-8"))
        selected = followup_targets(directory)
        existing = {r["key"] for r in summary["http_responses"]}
        selected = [t for t in selected if t[0] not in existing]
    else:
        summary = {"started_at": timestamp(), "scope": "Live public metadata only; HTTP success does not prove usable country chart rows.", "http_responses": []}
        selected = targets()
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(capture, *target, directory) for target in selected]
        for future in as_completed(futures):
            summary["http_responses"].append(future.result())
            (directory / "fetch-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if not args.followup:
        summary["http_responses"].extend(trends(directory))
    summary["completed_at"] = timestamp()
    (directory / "fetch-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output_dir": str(directory), "responses": len(summary["http_responses"])}), flush=True)


if __name__ == "__main__":
    main()
