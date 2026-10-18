"""Offline regression tests: failures, pagination, timestamps and source mapping."""
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "crawler"), str(ROOT / "shared")]

import common
import run_crawler
import src_apple
import src_spotify
import src_trends
import src_youtube as yt


class Response:
    def __init__(self, data=None, status=200, text=""):
        self.data, self.status_code, self.text = data, status, text

    def json(self):
        return self.data

    def raise_for_status(self):
        if self.status_code != 200:
            raise RuntimeError("HTTP error")


class Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []
        self.closed = False

    def get(self, url, **kwargs):
        self.calls.append((url, dict(kwargs.get("params", {}))))
        return next(self.responses)

    def close(self):
        self.closed = True


class Sink:
    def __init__(self):
        self.count = self.errors = 0
        self.rows = []
        self.closed = False

    def send(self, *row):
        self.rows.append(row)
        self.count += 1

    def close(self):
        self.closed = True


def comment(n, published="2026-10-18T12:00:00Z"):
    return {"id": f"thread-{n}", "snippet": {"topLevelComment": {
        "id": f"comment-{n}", "snippet": {"publishedAt": published, "textOriginal": "hello",
        "authorChannelId": {"value": "private-author"}}}}}


def test_incremental_comments_scan_beyond_200(monkeypatch):
    monkeypatch.setenv("YT_COMMENT_MAX_SCAN", "1000")
    pages = [Response({"items": [comment(i) for i in range(start, start + 100)],
                       "nextPageToken": str(start + 100)}) for start in (0, 100)]
    pages.append(Response({"items": [comment(i) for i in range(200, 250)]}))
    session = Session(pages)
    with common.capture_issues() as issues:
        rows = list(yt._api_comments(session, "secret", "video", 200, 1))
    assert len(rows) == 250
    assert rows[0]["comment_id"] == "comment-0"  # not thread ID
    assert rows[0]["author_hash"] != "private-author"
    assert session.calls[2][1]["pageToken"] == "200"
    assert not issues


def test_incremental_cutoff_and_overlap():
    session = Session([Response({"items": [comment(1), comment(1)], "nextPageToken": "p2"}),
                       Response({"items": [comment(2), comment(3, "2020-01-01T00:00:00Z")],
                                 "nextPageToken": "must-not-request"})])
    rows = list(yt._api_comments(session, "secret", "video", 1, 1700000000000))
    assert [x["comment_id"] for x in rows] == ["comment-1", "comment-2"]
    assert len(session.calls) == 2


def test_comment_safety_limit_is_reported(monkeypatch):
    monkeypatch.setenv("YT_COMMENT_MAX_SCAN", "2")
    session = Session([Response({"items": [comment(1), comment(2), comment(3)]})])
    with common.capture_issues() as issues:
        assert len(list(yt._api_comments(session, "key", "video", 200, 1))) == 2
    assert issues[0]["reason"] == "comment_window_truncated"


def test_repeated_page_token_terminates():
    response = Response({"items": [comment(1)], "nextPageToken": "same"})
    with common.capture_issues() as issues:
        assert len(list(yt._api_comments(Session([response, response]), "key", "v", 200, 1))) == 1
    assert issues[0]["reason"] == "repeated_page_token"


@pytest.mark.parametrize("reason,status,raises", [("commentsDisabled", 403, False),
    ("videoNotFound", 404, False), ("quotaExceeded", 403, True), ("forbidden", 403, True)])
def test_comment_http_errors_are_distinguished(reason, status, raises):
    session = Session([Response({"error": {"errors": [{"reason": reason}]}}, status)])
    if raises:
        with pytest.raises(RuntimeError, match="HTTP"):
            list(yt._api_comments(session, "secret", "video", 200, None))
    else:
        assert list(yt._api_comments(session, "secret", "video", 200, None)) == []


@pytest.mark.parametrize("issue,n,status", [(False, 1, "success"), (False, 0, "empty"),
                                            (True, 1, "partial"), (True, 0, "failed")])
def test_source_status(issue, n, status):
    def adapter(*_):
        for _ in range(n):
            yield "topic", "key", {}
        if issue:
            common.report_issue("child", "failed")
    result = run_crawler.run_source("charts", "source", adapter, None, Sink(), 1)
    assert result["status"] == status
    assert result["records"] == n


def test_runner_continues_after_failure_and_strict_exit(monkeypatch):
    def broken(*_):
        raise RuntimeError("url?key=secret")
    def good(*_):
        yield "topic", "key", {}
    monkeypatch.setattr(run_crawler, "JOBS", {"charts": [("bad", broken), ("good", good)]})
    for extra, expected in [([], 1), (["--allow-partial"], 0)]:
        sink, session = Sink(), Session([])
        monkeypatch.setattr(run_crawler, "make_sink", lambda *a, **k: sink)
        monkeypatch.setattr(run_crawler, "http_session", lambda: session)
        assert run_crawler.main(["--job", "charts", *extra]) == expected
        assert sink.count == 1 and sink.closed and session.closed


def test_runner_empty_incremental_comments(monkeypatch):
    monkeypatch.setattr(run_crawler, "JOBS", {"comments": [("youtube", lambda *a, **k: iter(()))]})
    monkeypatch.setattr(run_crawler, "make_sink", lambda *a, **k: Sink())
    monkeypatch.setattr(run_crawler, "http_session", lambda: Session([]))
    assert run_crawler.main(["--job", "comments", "--since-hours", "3"]) == 0


def test_runner_delivery_error_cannot_be_allowed(monkeypatch):
    sink = Sink()
    sink.errors = 1
    monkeypatch.setattr(run_crawler, "JOBS", {"charts": [("x", lambda *a: iter([("t", "k", {})]))]})
    monkeypatch.setattr(run_crawler, "make_sink", lambda *a, **k: sink)
    monkeypatch.setattr(run_crawler, "http_session", lambda: Session([]))
    assert run_crawler.main(["--job", "charts", "--allow-partial"]) == 1


def test_source_timestamp_never_inferred_from_crawl_time():
    record = common.chart_entry("chart", 1800000, 1, 1, {}, source_cadence="weekly")
    assert record["source_updated_ms"] is None
    assert record["source_freshness"] == "unknown"
    assert common.source_metadata(updated_at="2026-10-18")["source_updated_ms"] is None
    assert common.source_metadata(updated_at="bad")["source_updated_ms"] is None
    assert common.source_metadata(updated_at="Sun, 18 Oct 2026 08:59:19 +0000")["source_updated_ms"] == 1792313959000


def test_apple_source_timestamp_and_country(monkeypatch):
    monkeypatch.setenv("APPLE_COUNTRY", "us")
    session = Session([Response({"feed": {"updated": "2026-10-18T00:00:00Z", "results": [
        {"id": "1", "name": "Song", "artistName": "Artist"}]}})])
    row = list(src_apple.crawl_charts(session, 1))[0][2]
    assert row["chart_scope"] == "US"
    assert row["source_updated_ms"] > row["crawled_ms"]  # retains source value, never fabricates one


def test_kworb_rank_movement_markup_and_scope():
    rows = []
    for rank, move in [(1, '<span class="up">+2</span>'), (2, "-1"), (3, "="), (4, "NEW")]:
        cells = [str(rank), move, '<a href="../track/abc.html">Song &amp; A</a><a href="../artist/a">Artist</a>',
                 "1", "1", "1", "1,234"]
        opening = '<tr class="d2">' if rank == 4 else "<tr>"
        rows.append(opening + "".join("<td>" + v + "</td>" for v in cells) + "</tr>")
    session = Session([Response(text="<tbody>" + "".join(rows) + "</tbody>")])
    records = [r for _, _, r in src_spotify.crawl_kworb_daily(session, 1, "us")]
    assert [r["previous_rank"] for r in records] == [3, 1, 3, None]
    assert records[0]["metric_value"] == 1234
    assert records[0]["chart_scope"] == "US"
    assert src_spotify._int('<span data-value="987">1,234</span>') == 1234


def test_empty_spotify_children_are_not_success(monkeypatch):
    monkeypatch.setattr(src_spotify, "fetch_embed_playlist", lambda *a: ("empty", []))
    session = Session([Response(text="<tbody></tbody>")])
    result = run_crawler.run_source("charts", "spotify", src_spotify.crawl_charts, session, Sink(), 1)
    assert result["status"] == "failed" and len(result["issues"]) == 3


def test_youtube_snapshot_30_minutes_and_fallback_metric(monkeypatch):
    tr = {"track_key": "song__artist"}
    monkeypatch.setattr(yt, "chart_videos", lambda s: [("youtube_most_popular_music_vn", "v", tr, "42",
                                                    {"currentPosition": 1})])
    first = list(yt.crawl_charts(None, 0))[0][2]
    second = list(yt.crawl_charts(None, 1800000))[0][2]
    assert first["snapshot_id"] != second["snapshot_id"]
    assert first["metric_name"] == "view_count"
    assert first["source_cadence"] == "unknown"


def test_comments_deduplicate_between_sorts(monkeypatch):
    monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)
    monkeypatch.setenv("YT_COMMENT_ZING_EXTRA", "false")
    monkeypatch.setenv("YT_POPULAR_PER_VIDEO", "1")
    monkeypatch.setattr(yt.time, "sleep", lambda _: None)
    tr = {"track_key": "song__artist", "title": "Song", "artists": ["Artist"]}
    monkeypatch.setattr(yt, "chart_videos", lambda s: [("chart", "v", tr, 1, {})])
    monkeypatch.setattr(yt, "_scrape_comments", lambda *a: iter([{"comment_id": "same", "text": "Hello"}]))
    assert len(list(yt.crawl_comments(None, 1))) == 1


def test_scrape_window_scans_beyond_sample(monkeypatch):
    monkeypatch.setenv("YT_COMMENT_MAX_SCAN", "1000")
    comments = [{"cid": str(i), "time_parsed": 100, "text": "hello"} for i in range(250)]
    class Downloader:
        def get_comments(self, *args, **kwargs):
            return iter(comments)
    monkeypatch.setitem(sys.modules, "youtube_comment_downloader", SimpleNamespace(
        SORT_BY_POPULAR=0, SORT_BY_RECENT=1, YoutubeCommentDownloader=Downloader))
    assert len(list(yt._scrape_comments("v", 200, 1))) == 250


def test_trends_keeps_anchor_after_first_group_failure(monkeypatch):
    tracks = [{"track_key": f"song{i}__artist", "title": f"Song {i}", "artists": ["Artist"],
               "query": f"query{i}"} for i in range(6)]
    monkeypatch.setattr(src_trends, "candidates", lambda *a: tracks)
    monkeypatch.setattr(src_trends.time, "sleep", lambda _: None)
    class Client:
        calls = 0
        def compare_by_region(self, keywords):
            self.calls += 1
            if self.calls == 3:
                raise RuntimeError("temporary source failure")
            return [{"geoCode": "VN-HN", "geoName": "Ha Noi", "value": [20] * len(keywords),
                     "hasData": [True] * len(keywords)}]
    monkeypatch.setattr(src_trends, "TrendsClient", Client)
    with common.capture_issues() as issues:
        rows = [r for _, _, r in src_trends.crawl_trends(None, 1)]
    assert len(rows) == 2 and sum(r["is_anchor"] for r in rows) == 1
    assert all(r["group_id"] == 1 for r in rows)
    assert issues[0]["component"] == "trends_group:0"


def test_trends_malformed_region_is_visible(monkeypatch):
    tracks = [{"track_key": f"song{i}__artist", "title": f"Song {i}", "artists": [],
               "query": f"q{i}"} for i in range(2)]
    monkeypatch.setattr(src_trends, "candidates", lambda *a: tracks)
    monkeypatch.setattr(src_trends.time, "sleep", lambda _: None)
    class Client:
        def compare_by_region(self, keywords):
            return [{"geoCode": "VN-HN", "value": [20], "hasData": [True, True]}]
    monkeypatch.setattr(src_trends, "TrendsClient", Client)
    with common.capture_issues() as issues:
        assert list(src_trends.crawl_trends(None, 1)) == []
    assert issues[0]["reason"] == "invalid_region_values"


def test_max_pages_bounds_empty_pages(monkeypatch):
    monkeypatch.setenv("YT_COMMENT_MAX_PAGES", "2")
    session = Session([Response({"items": [], "nextPageToken": "a"}),
                       Response({"items": [], "nextPageToken": "b"})])
    with common.capture_issues() as issues:
        assert list(yt._api_comments(session, "k", "v", 200, 1)) == []
    assert len(session.calls) == 2 and issues[0]["reason"] == "comment_page_limit"


@pytest.mark.parametrize("rank,size", [(None, 10), (0, 10), (11, 10), (1, 0)])
def test_invalid_chart_rank_rejected(rank, size):
    with pytest.raises(ValueError):
        common.chart_entry("chart", 1, rank, size, {})


def test_blank_title_rejected():
    with pytest.raises(ValueError):
        common.track_fields(" ", ["Artist"], "source")


def test_source_exception_does_not_log_secret(caplog):
    def adapter(*args):
        raise RuntimeError("https://example.org?key=top-secret")
    with caplog.at_level("INFO"):
        result = run_crawler.run_source("charts", "source", adapter, None, Sink(), 1)
    assert result["status"] == "failed"
    assert "top-secret" not in caplog.text


def test_sink_flush_failure_is_failure(monkeypatch):
    class BrokenSink(Sink):
        def close(self):
            raise TimeoutError("flush failed")
    session = Session([])
    monkeypatch.setattr(run_crawler, "JOBS", {"charts": [("x", lambda *a: iter([("t", "k", {})]))]})
    monkeypatch.setattr(run_crawler, "make_sink", lambda *a, **k: BrokenSink())
    monkeypatch.setattr(run_crawler, "http_session", lambda: session)
    assert run_crawler.main(["--job", "charts", "--allow-partial"]) == 1
    assert session.closed
