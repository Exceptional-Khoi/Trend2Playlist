"""Unit tests cho parser/fallback của crawler, không gọi Internet."""
import os
import sys

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "crawler"))
sys.path.insert(0, os.path.join(ROOT, "shared"))

import src_youtube  # noqa: E402


class _Response:
    status_code = 200
    text = ""

    def json(self):
        return {
            "items": [{
                "id": "video-1",
                "snippet": {
                    "title": "DATG MUSIC | BÀI MỚI | OFFICIAL MUSIC VIDEO",
                    "channelTitle": "DatG Music",
                    "publishedAt": "2026-10-01T01:02:03Z",
                    "thumbnails": {"high": {"url": "https://img.example/high.jpg"}},
                },
                "contentDetails": {"duration": "PT3M42S"},
                "statistics": {"viewCount": "123456", "likeCount": "789"},
            }]
        }


class _Session:
    def get(self, url, params=None, timeout=None):
        assert url == "https://www.googleapis.com/youtube/v3/videos"
        assert params["chart"] == "mostPopular"
        assert params["videoCategoryId"] == "10"
        assert params["regionCode"] == "VN"
        return _Response()


def test_youtube_duration_parser():
    assert src_youtube._duration_seconds("PT3M42S") == 222
    assert src_youtube._duration_seconds("PT1H2M3S") == 3723
    assert src_youtube._duration_seconds("P1DT2S") == 86402
    assert src_youtube._duration_seconds(None) is None


def test_youtube_data_api_fallback_mapping():
    rows = src_youtube._api_popular_music(_Session(), "test-key")
    chart_id, video_id, track, views, meta = rows[0]
    assert chart_id == "youtube_most_popular_music_vn"
    assert video_id == "video-1"
    assert track["title"] == "BÀI MỚI"
    assert track["artists"] == ["DatG"]
    assert track["duration_s"] == 222
    assert track["release_date"] == "2026-10-01"
    assert views == "123456"
    assert meta["_total_plays"] == 123456
    assert meta["_total_likes"] == 789


def test_youtube_label_title_extracts_performers():
    title, artists = src_youtube._api_title_artists(
        "TRỄ GIỜ CƠM - Quang Hùng MasterD, WEAN, Sơn.K | TINH HÀ SAY HI", "Vie Channel -")
    assert title == "TRỄ GIỜ CƠM"
    assert artists == ["Quang Hùng MasterD", "WEAN", "Sơn.K"]
    title, artists = src_youtube._api_title_artists(
        "CONGB - chờ chút... | TINH HÀ SAY HI", "Vie Channel -")
    assert title == "chờ chút..."
    assert artists == ["CONGB"]


def test_youtube_chart_falls_back_with_api_key(monkeypatch):
    monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
    monkeypatch.setattr(src_youtube, "_internal_chart_videos", lambda _: (_ for _ in ()).throw(RuntimeError("429")))
    monkeypatch.setattr(src_youtube, "_api_popular_music", lambda _, __: ["fallback"])
    assert src_youtube.chart_videos(object()) == ["fallback"]


def test_comment_candidates_respect_total_limit(monkeypatch):
    monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)
    monkeypatch.setenv("YT_COMMENT_VIDEOS", "2")
    monkeypatch.setenv("YT_COMMENT_ZING_EXTRA", "true")
    monkeypatch.setattr(src_youtube, "chart_videos", lambda _: (_ for _ in ()).throw(RuntimeError("429")))
    monkeypatch.setattr(src_youtube, "_zing_candidates", lambda _: [
        {"track_key": f"song-{i}__artist", "title": f"Song {i}", "artists": ["Artist"]}
        for i in range(5)
    ])
    searched = []

    def no_result(_, query):
        searched.append(query)
        return None

    monkeypatch.setattr(src_youtube, "search_video", no_result)
    assert list(src_youtube.crawl_comments(object(), 1_000)) == []
    assert len(searched) == 2
