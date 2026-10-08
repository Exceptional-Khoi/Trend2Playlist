"""Compare saved public music snapshots; never sends network requests."""

from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import sys
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "crawler"))
from survey_audit_asian import NCT_URL, _nct
from survey_audit_western import (
    BANDCAMP_INITIAL, AUDIOMACK_PAGE, SOUNDCLOUD_PAGE,
    parse_bandcamp_feed, parse_soundcloud_html, parse_audiomack_html,
)

AUDIT = ROOT / "out/realtime-audit-2026-10-07"


def responses(directory):
    result = {}
    for path in (directory / "http").glob("*.json"):
        meta = json.loads(path.read_text(encoding="utf-8"))
        result[meta["url"]] = meta
    return result


def saved(directory, url):
    meta = responses(directory).get(url)
    if not meta:
        return None
    return meta, (directory / meta["body_file"]).read_text(encoding="utf-8")


class MemoryContext:
    """Reuse the NCT parser against cached metadata, with no networking."""
    def __init__(self, directory):
        self.store = SimpleNamespace(directory=directory)
        self.offline = True
        self.rows = []

    def get(self, url):
        item = saved(self.store.directory, url)
        if not item:
            raise ValueError("Missing saved response")
        meta, body = item
        return meta["status"], meta["headers"], body.encode("utf-8")

    def record(self, platform, category, payload, source_url):
        self.rows.append(payload)

    def attempt(self, platform, category, url, callback):
        callback()


def counter_observation(directory, kind, url):
    item = saved(directory, url)
    if not item:
        return None
    meta, body = item
    if meta["status"] != 200:
        return None
    if kind == "NhacCuaTui":
        context = MemoryContext(directory)
        _nct(context)
        data = context.rows[0]
        metrics = {k: data[k] for k in ("likes", "comments", "shares") if k in data}
    elif kind == "Audiomack":
        data = parse_audiomack_html(body)[0]
        metrics = data["stats"]
    else:
        data = parse_soundcloud_html(body)["counters"][0]
        metrics = {k: data[k] for k in ("playback_count", "likes_count", "reposts_count", "comment_count") if k in data}
    return {"platform": kind, "url": url, "requested_at": meta["requested_at"],
            "metrics": metrics, "evidence_file": str((directory / meta["body_file"]).relative_to(ROOT))}


def main():
    round1, round2 = AUDIT / "round1", AUDIT / "round2"
    summaries = [json.loads((d / "summary.json").read_text(encoding="utf-8")) for d in (round1, round2)]
    probes = [p for summary in summaries for p in summary.get("probe_attempts", [])]
    labels = sorted({p["platform"] for p in probes if p.get("platform")})
    data = {"scope": "Actual fresh HTTP probes and semantic public-metadata comparisons",
            "platforms_covered": len(labels), "platforms": labels, "network_probes": len(probes),
            "record_samples_by_round": [s["total_records"] for s in summaries],
            "counters": [], "charts": [], "bandcamp": {}}
    for platform, url in (("NhacCuaTui", NCT_URL), ("Audiomack", AUDIOMACK_PAGE), ("SoundCloud", SOUNDCLOUD_PAGE)):
        first = counter_observation(round1, platform, url)
        second = counter_observation(round2, platform, url)
        if first and second:
            delta = {k: second["metrics"][k] - value for k, value in first["metrics"].items()
                     if isinstance(value, (int, float)) and isinstance(second["metrics"].get(k), (int, float))}
            elapsed = (datetime.fromisoformat(second["requested_at"]) - datetime.fromisoformat(first["requested_at"])).total_seconds()
            history = []
            for old in (ROOT / "out/crawlKhoi-survey", ROOT / "out/realtime-recheck-2026-10-07"):
                observation = counter_observation(old, platform, url)
                if observation:
                    history.append(observation)
            data["counters"].append({"platform": platform, "first": first, "second": second,
                                     "elapsed_seconds": elapsed, "delta": delta, "earlier_observations": history,
                                     "interpretation": "Counter differences measure observed change; event-to-counter latency is not measured."})
    for chart_id in ("8246775932", "18176153161"):
        url = "https://music.163.com/discover/toplist?id=" + chart_id
        def chart(directory):
            meta, body = saved(directory, url)
            import re
            from html import unescape
            match = re.search(r'<textarea[^>]+id="song-list-pre-data"[^>]*>(.*?)</textarea>', body, re.S)
            rows = json.loads(unescape(match.group(1)))
            return {"requested_at": meta["requested_at"], "ids": [r["id"] for r in rows], "scores": [r.get("score") for r in rows]}
        observations = [chart(ROOT / "out/realtime-recheck-2026-10-07"), chart(round1), chart(round2)]
        data["charts"].append({"chart_id": chart_id, "source_url": url, "observations": observations,
                               "changed_positions_old_to_round1": sum(a != b for a, b in zip(observations[0]["ids"], observations[1]["ids"])),
                               "new_song_ids_old_to_round1": len(set(observations[1]["ids"]) - set(observations[0]["ids"])),
                               "changed_positions_round1_to_round2": sum(a != b for a, b in zip(observations[1]["ids"], observations[2]["ids"]))})
    first_meta, first_body = saved(round1, BANDCAMP_INITIAL)
    second_meta, second_body = saved(round2, BANDCAMP_INITIAL)
    first_feed, second_feed = json.loads(first_body), json.loads(second_body)
    first_rows = parse_bandcamp_feed(first_feed, limit=None)
    second_rows = parse_bandcamp_feed(second_feed, limit=None)
    cursor = first_feed.get("feed_data", first_feed)["end_date"]
    incremental_url = "https://bandcamp.com/api/salesfeed/1/get?start_date=" + str(cursor)
    inc_meta, inc_body = saved(round2, incremental_url)
    inc_feed = json.loads(inc_body)
    inc_rows = parse_bandcamp_feed(inc_feed, limit=None)
    server_time = second_feed.get("feed_data", second_feed)["server_time"]
    latest_sale = max(r["event_timestamp_seconds"] for r in second_rows)
    data["bandcamp"] = {
        "source_url": BANDCAMP_INITIAL, "first_requested_at": first_meta["requested_at"],
        "second_requested_at": second_meta["requested_at"], "first_items": len(first_rows), "second_items": len(second_rows),
        "first_types": dict(Counter(r["product_type"] for r in first_rows)),
        "second_types": dict(Counter(r["product_type"] for r in second_rows)),
        "overlap_item_fingerprints": len({r["sale_fingerprint"] for r in first_rows} & {r["sale_fingerprint"] for r in second_rows}),
        "incremental_source_url": incremental_url, "incremental_items": len(inc_rows),
        "incremental_types": dict(Counter(r["product_type"] for r in inc_rows)),
        "incremental_music_items": sum(r["product_type"] in {"track", "album", "discography"} for r in inc_rows),
        "incremental_items_after_old_cursor": sum(r["event_timestamp_seconds"] > cursor for r in inc_rows),
        "source_server_time": server_time, "latest_sale_time": latest_sale,
        "latest_sale_age_vs_server_seconds": round(server_time - latest_sale, 2),
        "public_client_poll_seconds": 32, "public_ui_display_buffer_seconds": 120,
        "interpretation": "Public sale events, not listens. UI buffering is not a guaranteed API publication latency; fingerprints are composite identities, not native event IDs."}
    failures = [p for p in probes if p.get("error") or (p.get("status") or 0) >= 400]
    data["transport_or_http_failures"] = failures
    for directory, summary in zip((round1, round2), summaries):
        actual = 0
        for platform in summary["platforms"]:
            if platform.get("data_file"):
                rows = [json.loads(line) for line in (directory / platform["data_file"]).read_text(encoding="utf-8").splitlines() if line.strip()]
                if len(rows) != platform["records"]:
                    raise ValueError("JSONL/summary count mismatch")
                actual += len(rows)
        if actual != summary["total_records"]:
            raise ValueError("Total JSONL/summary count mismatch")
    output = AUDIT / "comparison.json"
    output.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"comparison_file": str(output), "platforms_covered": len(labels), "network_probes": len(probes),
                      "samples_by_round": data["record_samples_by_round"],
                      "counter_deltas": [{"platform": c["platform"], "elapsed_seconds": c["elapsed_seconds"], "delta": c["delta"]} for c in data["counters"]],
                      "bandcamp_incremental_items": len(inc_rows)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
