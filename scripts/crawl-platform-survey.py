"""Collect a bounded sample of public music metadata and keep request evidence.

Run from the repository root. Each adapter writes independent JSONL files;
the JSON summary describes this run, including failed and empty attempts.
"""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import importlib
import json
from pathlib import Path
import re
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "tmp/crawl-deps"), str(ROOT / "crawler"), str(ROOT / "shared")]

import requests


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def slug(value):
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


class Store:
    def __init__(self, directory, platforms=None, resume=False):
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        (directory / "http").mkdir(exist_ok=True)
        (directory / "records").mkdir(exist_ok=True)
        self.lock = threading.Lock()
        self.platforms = {p.casefold() for p in platforms or []}
        self.attempts = []
        self.responses = []
        self.probes = []
        self.records = defaultdict(list)
        self.started_at = timestamp()
        summary_path = directory / "summary.json"
        if resume and summary_path.exists():
            previous = json.loads(summary_path.read_text(encoding="utf-8"))
            self.started_at = previous.get("started_at", self.started_at)
            self.responses = previous.get("http_responses", [])
            self.probes = previous.get("probe_attempts", [])
            for item in previous.get("platforms", []):
                platform = item["platform"]
                self.attempts.extend(item.get("attempts", []))
                if item.get("data_file"):
                    path = directory / item["data_file"]
                    if path.exists():
                        self.records[platform] = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
            # A targeted rerun replaces those sample files and statistics.
            # Their earlier attempts remain in a separate history file.
            if self.platforms:
                history = [a for a in self.attempts if self.allowed(a["platform"])]
                if history:
                    with (directory / "attempt-history.jsonl").open("a", encoding="utf-8") as stream:
                        for item in history:
                            stream.write(json.dumps(item, ensure_ascii=False) + "\n")
                self.attempts = [a for a in self.attempts if not self.allowed(a["platform"])]
                for platform in list(self.records):
                    if self.allowed(platform):
                        self.records[platform] = []
                        path = directory / "records" / (slug(platform) + ".jsonl")
                        if path.exists():
                            path.write_text("", encoding="utf-8")
        elif any((directory / "records").glob("*.jsonl")):
            raise ValueError("Output already has samples. Use --resume with --platforms, or a new --output-dir.")

    def allowed(self, platform):
        return not self.platforms or platform.casefold() in self.platforms

    def add_record(self, platform, category, payload, source_url):
        if not isinstance(payload, dict):
            raise TypeError("Record payload must be a JSON object")
        record = {"platform": platform, "category": category, "crawled_at": timestamp(),
                  "source_url": source_url, "data": payload}
        encoded = json.dumps(record, ensure_ascii=False)
        with self.lock:
            self.records[platform].append(record)
            with (self.directory / "records" / (slug(platform) + ".jsonl")).open("a", encoding="utf-8") as stream:
                stream.write(encoded + "\n")

    def save(self):
        platforms = sorted({a["platform"] for a in self.attempts} | set(self.records), key=str.casefold)
        summaries = []
        for platform in platforms:
            records = self.records[platform]
            attempts = [a for a in self.attempts if a["platform"] == platform]
            failures = [a for a in attempts if a["status"] != "success"]
            fields = Counter()
            identities = set()
            for record in records:
                data = record["data"]
                fields.update(key for key, value in data.items() if value is not None and value != "" and value != [] and value != {})
                identity = data.get("platform_id") or data.get("id") or data.get("url") or data.get("identifier")
                if identity:
                    identities.add(str(identity))
            summaries.append({
                "platform": platform,
                "status": "partial" if records and failures else "success" if records else "failed",
                "records": len(records),
                "unique_top_level_ids_or_urls": len(identities),
                "categories": dict(Counter(r["category"] for r in records)),
                "fields_present": dict(sorted(fields.items())),
                "source_urls": sorted({r["source_url"] for r in records}),
                "data_file": "records/" + slug(platform) + ".jsonl" if records else None,
                "attempts": attempts,
            })
        result = {"started_at": self.started_at, "completed_at": timestamp(),
                  "scope": "Bounded public metadata samples; counts are records, not unique songs.",
                  "platforms_attempted": len(summaries),
                  "platforms_with_records": sum(bool(p["records"]) for p in summaries),
                  "total_records": sum(p["records"] for p in summaries),
                  "platforms": summaries,
                  "http_responses": self.responses,
                  "probe_attempts": self.probes}
        (self.directory / "summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result


class Context:
    def __init__(self, store, timeout=20, refresh=False, offline=False):
        self.store = store
        self.timeout = timeout
        self.refresh = refresh
        self.offline = offline
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.8,vi;q=0.7",
        })

    def response(self, method, url, **kwargs):
        request = requests.Request(method, url, params=kwargs.pop("params", None),
                                   json=kwargs.pop("json", None), data=kwargs.pop("data", None),
                                   headers=kwargs.pop("headers", None))
        prepared = self.session.prepare_request(request)
        body = prepared.body or b""
        if isinstance(body, str):
            body = body.encode("utf-8")
        identity = hashlib.sha256(method.encode() + prepared.url.encode() + body).hexdigest()[:20]
        meta_path = self.store.directory / "http" / (identity + ".json")
        body_path = self.store.directory / "http" / (identity + ".body")
        if not self.refresh and meta_path.exists() and body_path.exists():
            metadata = json.loads(meta_path.read_text(encoding="utf-8"))
            response = requests.Response()
            response.status_code = metadata["status"]
            response.headers.update(metadata["headers"])
            response.url = metadata.get("final_url", prepared.url)
            response.encoding = metadata.get("encoding") or "utf-8"
            response._content = body_path.read_bytes()
            metadata = {**metadata, "cache_used": True}
        else:
            if self.offline:
                raise ValueError("No cached response available in offline mode")
            settings = self.session.merge_environment_settings(prepared.url, {}, False, None, None)
            response = self.session.send(prepared, timeout=kwargs.pop("timeout", self.timeout), **settings)
            media_type = response.headers.get("Content-Type", "").lower()
            if media_type.startswith(("audio/", "video/")):
                response.close()
                raise ValueError("Expected a metadata response, received audio/video")
            if len(response.content) > 15 * 1024 * 1024:
                raise ValueError("Metadata response exceeds 15 MiB sample limit")
            response.encoding = "utf-8"
            body_path.write_bytes(response.content)
            metadata = {"requested_at": timestamp(), "method": method, "url": prepared.url,
                        "final_url": response.url, "status": response.status_code,
                        "headers": {key: value for key, value in response.headers.items()
                                    if key.lower() in {"content-type", "content-length", "date", "last-modified"}},
                        "encoding": response.encoding, "bytes": len(response.content),
                        "body_file": "http/" + body_path.name}
            meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        with self.store.lock:
            self.store.responses.append(metadata)
        return response

    def get(self, url, headers=None):
        response = self.response("GET", url, headers=headers)
        return response.status_code, dict(response.headers), response.content

    def post_json(self, url, payload, headers=None):
        response = self.response("POST", url, json=payload, headers=headers)
        return response.status_code, dict(response.headers), response.content

    def record(self, platform, category, payload, source_url):
        if self.store.allowed(platform):
            self.store.add_record(platform, category, payload, source_url)

    def attempt(self, platform, category, url, callback):
        if not self.store.allowed(platform):
            return
        before = len(self.store.records[platform])
        started = time.monotonic()
        error = None
        try:
            callback()
        except Exception as exc:
            error = type(exc).__name__ + ": " + str(exc)
        count = len(self.store.records[platform]) - before
        status = "success" if count and error is None else "partial" if count else "failed" if error else "empty"
        result = {"platform": platform, "category": category, "url": url, "status": status,
                  "records": count, "elapsed_seconds": round(time.monotonic() - started, 2), "error": error}
        with self.store.lock:
            self.store.attempts.append(result)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return result


class SessionProxy:
    """Use existing crawlers while capturing their HTTP response evidence."""
    def __init__(self, context):
        self.context = context
        self.cookies = context.session.cookies
        self.headers = context.session.headers

    def get(self, url, **kwargs):
        return self.context.response("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self.context.response("POST", url, **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="out/crawlKhoi-survey")
    parser.add_argument("--groups", default="primary,western,asian,other,extra")
    parser.add_argument("--platforms", default="")
    parser.add_argument("--probe-url", action="append", default=[])
    parser.add_argument("--probe-manifest", help="JSON array of labelled public metadata requests")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--resume", action="store_true", help="Keep existing platforms and replace those selected by --platforms")
    parser.add_argument("--offline", action="store_true", help="Reprocess only responses already collected; make no network requests")
    args = parser.parse_args()
    probes = [{"url": url, "method": "GET"} for url in args.probe_url]
    if args.probe_manifest:
        manifest_path = Path(args.probe_manifest)
        if not manifest_path.is_absolute():
            manifest_path = ROOT / manifest_path
        entries = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(entries, list):
            parser.error("--probe-manifest must contain a JSON array")
        for entry in entries:
            if not isinstance(entry, dict) or not str(entry.get("url", "")).startswith("https://"):
                parser.error("Each manifest entry needs an HTTPS URL")
            if entry.get("method", "GET") not in {"GET", "POST"}:
                parser.error("Probe methods are GET or POST")
        probes.extend(entries)
    directory = Path(args.output_dir)
    if not directory.is_absolute():
        directory = ROOT / directory
    if args.resume and not args.platforms and not probes:
        parser.error("--resume requires --platforms to avoid duplicate samples")
    store = Store(directory, [p.strip() for p in args.platforms.split(",") if p.strip()], args.resume)
    if probes:
        def probe(entry):
            url = entry["url"]
            context = Context(store, args.timeout, args.refresh, args.offline)
            started = time.monotonic()
            result = {"platform": entry.get("platform"), "category": entry.get("category"),
                      "url": url, "method": entry.get("method", "GET"),
                      "observed_at": timestamp(), "status": None, "error": None}
            try:
                if result["method"] == "POST":
                    status, headers, body = context.post_json(url, entry.get("json", {}), entry.get("headers"))
                else:
                    status, headers, body = context.get(url, entry.get("headers"))
                result.update({"status": status, "bytes": len(body),
                               "body_sha256": hashlib.sha256(body).hexdigest()})
            except Exception as exc:
                result["error"] = type(exc).__name__ + ": " + str(exc)
            result["elapsed_seconds"] = round(time.monotonic() - started, 2)
            with store.lock:
                store.probes.append(result)
            print(json.dumps(result, ensure_ascii=False), flush=True)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(probe, probes))
        store.save()
        return

    def collect(group):
        module = importlib.import_module("survey_" + group)
        module.collect(Context(store, args.timeout, args.refresh, args.offline))

    groups = [g.strip() for g in args.groups.split(",") if g.strip()]
    with ThreadPoolExecutor(max_workers=min(4, len(groups))) as pool:
        futures = {pool.submit(collect, group): group for group in groups}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(json.dumps({"group": futures[future], "error": str(exc)}, ensure_ascii=False), flush=True)
    result = store.save()
    print(json.dumps({"summary_file": str(directory / "summary.json"),
                      "platforms_attempted": result["platforms_attempted"],
                      "platforms_with_records": result["platforms_with_records"],
                      "total_records": result["total_records"]}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
