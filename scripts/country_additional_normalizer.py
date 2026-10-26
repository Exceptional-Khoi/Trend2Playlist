"""Normalize captured country-source evidence; performs no network requests."""
from collections import Counter
from datetime import datetime, timezone
import json

from country_kkbox_parser import parse_kkbox
from country_rias_parser import parse_rias


def document(path):
    text = path.read_text(encoding="utf-8")
    # Google Trends returns an XSSI prefix before the JSON object.
    if text.startswith(")]}\'"):
        text = text.split("\n", 1)[1]
    return json.loads(text)


def validate(rows, expected_size=None):
    issues = []
    if expected_size is not None and len(rows) != expected_size:
        issues.append(f"Expected {expected_size} chart rows, got {len(rows)}")
    ranks = [r.get("rank") for r in rows]
    if sorted(ranks) != list(range(1, len(rows) + 1)):
        issues.append("Ranks are not unique and contiguous from 1")
    if any(not r.get("title") or not r.get("artists") for r in rows):
        issues.append("Missing title or artist attribution")
    ids = [r["platform_id"] for r in rows if r.get("platform_id")]
    if len(ids) != len(set(ids)):
        issues.append("Duplicate platform IDs within chart")
    return issues


def normalize(directory):
    fetch = document(directory / "fetch-summary.json")
    responses = {r["key"]: r for r in fetch["http_responses"]}
    records_dir = directory / "records"
    records_dir.mkdir(exist_ok=True)
    charts, files, all_rows, kkbox_charts = [], [], [], []

    def save(name, rows):
        path = records_dir / (name + ".jsonl")
        path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
        files.append({"file": path.relative_to(directory).as_posix(), "record_count": len(rows)})

    for key, response in responses.items():
        if response.get("source") != "kkbox" or response["status"] != "success":
            continue
        try:
            html = (directory / response["raw_file"]).read_text(encoding="utf-8")
            rows, metadata = parse_kkbox(html, response)
            metadata["validation_issues"] = validate(rows, 50)
            charts.append(metadata)
            kkbox_charts.append((metadata, rows))
            save(key, rows)
            all_rows.extend(rows)
        except Exception as error:
            charts.append({"key": key, "status": "parse_failed", "error": str(error)})

    if responses.get("rias_sg", {}).get("status") == "success":
        response = responses["rias_sg"]
        html = (directory / response["raw_file"]).read_text(encoding="utf-8")
        ajax_response = responses.get("rias_sg_main", {})
        ajax = document(directory / ajax_response["raw_file"]) if ajax_response.get("status") == "success" else None
        rows, metadata = parse_rias(html, response, ajax)
        for chart in metadata:
            selected = [r for r in rows if r["chart_kind"] == chart["chart_kind"]]
            chart["validation_issues"] = validate(selected, 20)
            if chart["chart_kind"] == "national" and ajax is not None:
                chart["data_url"] = ajax_response["source_url"]
                chart["data_crawled_at"] = ajax_response["requested_at"]
                for row in selected:
                    row.update({"data_url": chart["data_url"], "data_crawled_at": chart["data_crawled_at"]})
            save("rias_sg_" + chart["chart_kind"], selected)
        charts.extend(metadata)
        all_rows.extend(rows)

    trends_summary = {"status": "unavailable"}
    geo_response = responses.get("trends_geo", {})
    if geo_response.get("status") == "success":
        geo_rows = document(directory / geo_response["raw_file"])["default"]["geoMapData"]
        explore = document(directory / responses["trends_explore"]["raw_file"])
        request = next(w["request"] for w in explore["widgets"] if w["id"] == "GEO_MAP")
        periods = [item["time"].replace("\\", "") for item in request["comparisonItem"]]
        keywords = geo_response["keywords"]
        normalized = []
        for region in geo_rows:
            for i, keyword in enumerate(keywords):
                has_data = region["hasData"][i]
                normalized.append({
                    "source": "google_trends", "country_code": region["geoCode"],
                    "country_name": region["geoName"], "chart_scope": region["geoCode"],
                    "keyword": keyword, "keyword_index": i,
                    "metric": "query_comparison_share_percent_within_country",
                    "data_mode": request["dataMode"], "search_property": geo_response["property"],
                    "value": region["value"][i] if has_data else None,
                    "formatted_value": region["formattedValue"][i] if has_data else None,
                    "has_data": has_data, "source_period": periods[i],
                    "timeframe": geo_response["timeframe"], "period_type": "rolling_7_days",
                    "source_url": geo_response["source_url"], "crawled_at": geo_response["requested_at"],
                })
        usable = [r for r in normalized if r["has_data"]]
        save("google_trends_country_all", normalized)
        save("google_trends_country_with_data", usable)
        trends_summary = {
            "status": "parsed", "countries_returned": len(geo_rows),
            "countries_with_data": len({r["country_code"] for r in usable}),
            "observations_returned": len(normalized), "observations_with_data": len(usable),
            "keywords": keywords, "source_period": periods[0],
            "focus_countries": [r for r in normalized if r["country_code"] in {"VN", "TH", "ID", "US", "JP"}],
            "note": "Percentages compare the three queried terms within each country. They are not search counts or cross-country audience sizes; missing data is null, and a rounded 0 with has_data=true can mean <1%.",
        }

    comparisons = []
    for index, (left, left_rows) in enumerate(kkbox_charts):
        for right, right_rows in kkbox_charts[index + 1:]:
            if left.get("category_group") != "mandarin" or right.get("category_group") != "mandarin":
                continue
            if left["source_period"] != right["source_period"]:
                continue
            a = {r["platform_id"] for r in left_rows}
            b = {r["platform_id"] for r in right_rows}
            comparisons.append({"source": "kkbox", "countries": [left["country_code"], right["country_code"]],
                                "source_period": left["source_period"], "category_group": "mandarin",
                                "top_n": 50, "shared_track_ids": len(a & b), "union_track_ids": len(a | b),
                                "jaccard": len(a & b) / len(a | b)})

    summary = {
        "normalized_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "crawl_started_at": fetch["started_at"], "crawl_completed_at": fetch["completed_at"],
        "chart_record_count": len(all_rows),
        "chart_counts_by_source": dict(Counter(r["source"] for r in all_rows)),
        "charts": charts, "google_trends": trends_summary, "comparisons": comparisons,
        "record_files": files,
        "unavailable_sources": [{k: r.get(k) for k in ("key", "source", "country_code", "http_status", "error")}
                                for r in responses.values() if r["status"] != "success"],
        "official_sea_hub": {"http_status": responses.get("sea_weekly", {}).get("http_status"),
                             "chart_rows": 0, "note": "Captured HTML contains UI metadata; no country song rows extracted."},
    }
    (directory / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output_dir": str(directory), "chart_records": len(all_rows),
                      "trends_observations_with_data": trends_summary.get("observations_with_data", 0),
                      "comparisons": comparisons}), flush=True)
    return summary
