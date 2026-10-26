"""Parse captured RIAS country charts with the Python standard library.

This module only parses supplied HTML and JSON. It does not fetch URLs. RIAS
table row IDs identify source rows, not songs. Artist attribution is preserved
as one string because commas also occur within the displayed attribution.
"""

from datetime import date
from html.parser import HTMLParser
import json
import re
from urllib.parse import urljoin, urlparse


SOURCE_URL = "https://www.rias.org.sg/the-official-singapore-charts/"
MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
DATE_RANGE = re.compile(
    r"(?P<start_day>\d{1,2})\s+(?P<start_month>[A-Za-z]+)"
    r"(?:\s+(?P<start_year>\d{2}|\d{4}))?\s*[-\u2013\u2014]\s*"
    r"(?P<end_day>\d{1,2})\s+(?P<end_month>[A-Za-z]+)"
    r"(?:\s+(?P<end_year>\d{4}|\d{2}))?"
)


def _cell():
    return {"text": [], "images": []}


def _text(cell):
    return " ".join("".join(cell["text"]).split())


class _Fragment(HTMLParser):
    """Capture visible text and image URLs from one supplied cell fragment."""

    def __init__(self, html):
        super().__init__(convert_charrefs=True)
        self.cell = _cell()
        self.feed(str(html or ""))
        self.close()

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "img" and attrs.get("src"):
            self.cell["images"].append(attrs["src"])
        if tag in {"br", "p", "div", "h3", "gdiv"}:
            self.cell["text"].append(" ")

    def handle_endtag(self, tag):
        if tag in {"p", "div", "h3", "gdiv"}:
            self.cell["text"].append(" ")

    def handle_data(self, data):
        self.cell["text"].append(data)


class _Tables(HTMLParser):
    """Read table rows and subtitles without evaluating page JavaScript."""

    def __init__(self, html):
        super().__init__(convert_charrefs=True)
        self.tables = {}
        self.subtitles = []
        self.table_id = None
        self.table_depth = 0
        self.row = None
        self.cell = None
        self.subtitle_depth = 0
        self.subtitle_parts = []
        self.feed(html)
        self.close()

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if self.subtitle_depth:
            self.subtitle_depth += 1
        elif tag == "span" and "subtitle" in attrs.get("class", "").split():
            self.subtitle_depth = 1
            self.subtitle_parts = []
        if tag == "table":
            if self.table_depth:
                self.table_depth += 1
            else:
                self.table_depth = 1
                self.table_id = attrs.get("data-footable_id")
                if not self.table_id:
                    found = re.fullmatch(r"footable_(\d+)", attrs.get("id", ""))
                    self.table_id = found.group(1) if found else None
                if self.table_id:
                    self.tables.setdefault(self.table_id, [])
        if self.table_depth == 1 and self.table_id:
            if tag == "tr":
                self.row = {"source_row_id": attrs.get("data-row_id"), "cells": []}
            elif tag == "td" and self.row is not None:
                self.cell = _cell()
                self.row["cells"].append(self.cell)
        if self.cell is not None:
            if tag == "img" and attrs.get("src"):
                self.cell["images"].append(attrs["src"])
            if tag in {"br", "p", "div", "h3", "gdiv"}:
                self.cell["text"].append(" ")

    def handle_endtag(self, tag):
        if self.subtitle_depth:
            self.subtitle_depth -= 1
            if not self.subtitle_depth:
                self.subtitles.append(" ".join("".join(self.subtitle_parts).split()))
        if tag == "table":
            self.table_depth = max(0, self.table_depth - 1)
            if not self.table_depth:
                self.table_id = None
                self.row = None
                self.cell = None
        if self.table_depth == 1:
            if tag == "td":
                self.cell = None
            elif tag == "tr" and self.row is not None:
                if self.row["cells"]:
                    self.tables[self.table_id].append(self.row)
                self.row = None
                self.cell = None
        if self.cell is not None and tag in {"p", "div", "h3", "gdiv"}:
            self.cell["text"].append(" ")

    def handle_data(self, data):
        if self.cell is not None:
            self.cell["text"].append(data)
        if self.subtitle_depth:
            self.subtitle_parts.append(data)


def _configs(html):
    decoder = json.JSONDecoder()
    pattern = r"window\[(['\"])ninja_table_instance_\d+\1\]\s*=\s*"
    for match in re.finditer(pattern, html):
        try:
            config, _ = decoder.raw_decode(html, match.end())
        except json.JSONDecodeError:
            continue
        if isinstance(config, dict):
            yield config


def _year(value):
    if value is None:
        return None
    result = int(value)
    return result + 2000 if result < 100 else result


def _period(title, subtitles):
    week_match = re.search(r"\bWeek\s+(\d{1,2})\b", title, re.I)
    chart_week = int(week_match.group(1)) if week_match else None
    candidates = [title] + [
        text for text in subtitles
        if chart_week is None or re.search(rf"\bWeek\s+{chart_week}\b", text, re.I)
    ]
    for text in candidates:
        match = DATE_RANGE.search(text)
        if not match:
            continue
        values = match.groupdict()
        start_month = MONTHS.get(values["start_month"][:3].lower())
        end_month = MONTHS.get(values["end_month"][:3].lower())
        if not start_month or not end_month:
            continue
        start_year = _year(values["start_year"])
        end_year = _year(values["end_year"])
        # A title without a year is resolved only from another matching source
        # subtitle. The crawl date is never used as the native chart year.
        if start_year is None and end_year is None:
            continue
        rolls_year = start_month > end_month
        if start_year is None:
            start_year = end_year - int(rolls_year)
        if end_year is None:
            end_year = start_year + int(rolls_year)
        try:
            start = date(start_year, start_month, int(values["start_day"]))
            end = date(end_year, end_month, int(values["end_day"]))
        except ValueError:
            continue
        if start > end:
            continue
        return chart_week, end.year, start.isoformat(), end.isoformat()
    return chart_week, None, None, None


def _direction(url):
    if not url:
        return None
    name = urlparse(url).path.rsplit("/", 1)[-1].lower()
    if re.search(r"(?:^|[-_])up(?:[-_.]|$)", name):
        return "up"
    if re.search(r"(?:^|[-_])down(?:[-_.]|$)", name):
        return "down"
    if re.search(r"(?:^|[-_])stay(?:[-_.]|$)", name):
        return "stay"
    # A star image lacks an explicit textual movement label, so it remains
    # unknown. We do not infer a numerical previous rank from an icon.
    return None


def _record(cells, row_id, metadata):
    if len(cells) < 5:
        return None
    rank_match = re.match(r"\s*(\d+)\b", _text(cells[0]))
    title = _text(cells[2])
    attribution = _text(cells[3])
    if not rank_match or not title or not attribution:
        return None
    rank = int(rank_match.group(1))
    if rank < 1:
        return None
    movement_url = cells[0]["images"][0] if cells[0]["images"] else None
    cover_url = cells[1]["images"][0] if cells[1]["images"] else None
    result = {
        key: metadata[key] for key in (
            "source", "country_code", "chart_scope", "chart_kind", "chart_name",
            "period_type", "source_period", "chart_year", "chart_week",
            "chart_period_start", "chart_period_end", "source_url", "crawled_at",
        )
    }
    result.update({
        "platform_id": None,
        "source_row_id": str(row_id) if row_id is not None else None,
        "title": title,
        "artists": [attribution],
        "label": _text(cells[4]) or None,
        "rank": rank,
        "rank_previous": None,
        "movement_direction": _direction(movement_url),
        "movement_image_url": urljoin(SOURCE_URL, movement_url) if movement_url else None,
        "cover_url": urljoin(SOURCE_URL, cover_url) if cover_url else None,
        "streams": None,
    })
    return result


def parse_rias(html, response_metadata, ajax_document=None):
    """Return ``(records, chart_metadata_list)`` from captured RIAS evidence.

    ``ajax_document`` is the parsed JSON array returned by the national table's
    public data URL. No national rows are produced without that actual array.
    ``response_metadata`` supplies the original source URL and capture time.
    """
    parser = _Tables(html)
    records = []
    charts = []
    for config in _configs(html):
        table_id = str(config.get("table_id", ""))
        title = config.get("title") or ""
        if table_id == "3920" or re.match(r"Regional\b", title, re.I):
            kind = "regional"
        elif table_id == "3919" or re.match(r"Top\s*20\b", title, re.I):
            kind = "national"
        else:
            continue
        week, year, start, end = _period(title, parser.subtitles)
        metadata = {
            "source": "official_sea_rias",
            "country_code": "SG",
            "chart_scope": "SG",
            "chart_kind": kind,
            "chart_name": title,
            "period_type": "weekly",
            "source_period": f"{start}/{end}" if start and end else None,
            "chart_year": year,
            "chart_week": week,
            "chart_period_start": start,
            "chart_period_end": end,
            "source_url": response_metadata.get("source_url") or SOURCE_URL,
            "crawled_at": response_metadata.get("crawled_at") or response_metadata.get("requested_at"),
            "source_table_id": table_id,
            "data_request_url": config.get("init_config", {}).get("data_request_url"),
        }
        parsed = []
        if kind == "national" and isinstance(ajax_document, list):
            for item in ajax_document:
                if not isinstance(item, dict) or not isinstance(item.get("value"), dict):
                    continue
                values = item["value"]
                cells = [_Fragment(values.get(key)).cell for key in (
                    "singles", "cover", "title", "artist", "record_label",
                )]
                record = _record(cells, values.get("___id___"), metadata)
                if record:
                    parsed.append(record)
        elif kind == "regional":
            for row in parser.tables.get(table_id, []):
                record = _record(row["cells"], row["source_row_id"], metadata)
                if record:
                    parsed.append(record)
        parsed.sort(key=lambda row: row["rank"])
        metadata["record_count"] = len(parsed)
        metadata["status"] = "parsed" if parsed else "no_rows"
        metadata["data_origin"] = (
            "captured_ajax" if isinstance(ajax_document, list) else "missing_ajax"
        ) if kind == "national" else "captured_html"
        if kind == "national" and ajax_document is None:
            metadata["note"] = "National rows require the captured public AJAX response."
        charts.append(metadata)
        records.extend(parsed)
    return records, charts
