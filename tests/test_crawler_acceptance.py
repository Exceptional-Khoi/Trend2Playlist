"""Regression tests for delivery failures, isolated chart endpoints and conservative parsing."""
import sys
from pathlib import Path
from types import SimpleNamespace
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'crawler'), str(ROOT / 'shared')]
import common
import run_crawler
import src_youtube as yt


@pytest.mark.parametrize('fail_after', [0, 1])
def test_allow_partial_must_not_hide_synchronous_sink_failure(monkeypatch, fail_after):
    class Sink:
        count = errors = 0
        def send(self, *args):
            if self.count == fail_after:
                raise OSError('delivery failed')
            self.count += 1
        def close(self):
            pass
    sink = Sink()
    def adapter(*args):
        yield 'topic', 'key1', {}
        yield 'topic', 'key2', {}
    monkeypatch.setattr(run_crawler, 'JOBS', {'charts': [('source', adapter)]})
    monkeypatch.setattr(run_crawler, 'make_sink', lambda *a, **k: sink)
    monkeypatch.setattr(run_crawler, 'http_session', lambda: SimpleNamespace(close=lambda: None))
    assert run_crawler.main(['--job', 'charts', '--allow-partial']) == 1


def test_youtube_keep_valid_tracks_if_video_chart_fails(monkeypatch):
    monkeypatch.delenv('YOUTUBE_API_KEY', raising=False)
    def browse(session, chart_type):
        if chart_type == 'TRACKS':
            return [{'encryptedVideoId': 'v1', 'name': 'Song', 'artists': [{'name': 'Artist'}],
                     'chartEntryMetadata': {'currentPosition': 1}, 'viewCount': 100}]
        raise RuntimeError('video chart unavailable')
    monkeypatch.setattr(yt, '_browse', browse)
    with common.capture_issues() as issues:
        result = run_crawler.run_source('charts', 'youtube', yt.crawl_charts, None,
            SimpleNamespace(send=lambda *a: None), 1)
    assert result['records'] == 1
    assert result['status'] == 'partial'


def test_old_replies_must_not_silently_hide_new_top_level_comments(monkeypatch):
    comments = [{'cid': 'new1', 'time_parsed': 1000, 'text': 'New'}]
    comments += [{'cid': f'old{i}', 'time_parsed': 1, 'text': 'Old reply', 'reply': True}
                 for i in range(20)]
    comments += [{'cid': 'new2', 'time_parsed': 1001, 'text': 'New again'}]
    class Downloader:
        def get_comments(self, *args, **kwargs):
            return iter(comments)
    monkeypatch.setitem(sys.modules, 'youtube_comment_downloader', SimpleNamespace(
        SORT_BY_POPULAR=0, SORT_BY_RECENT=1, YoutubeCommentDownloader=Downloader))
    with common.capture_issues() as issues:
        rows = list(yt._scrape_comments('v', 200, 500000))
    assert {r['comment_id'] for r in rows} == {'new1', 'new2'}
    assert not issues


def test_label_video_title_not_swapped_with_single_artist():
    title, artists = yt._api_title_artists('Tìm Em - Hngle | Official MV', 'Vie Channel')
    assert title == 'Tìm Em - Hngle'
    assert artists == []


def test_youtube_keep_videos_when_tracks_fail_even_with_api_key(monkeypatch):
    monkeypatch.setenv('YOUTUBE_API_KEY', 'test-only-key')
    def browse(session, chart_type):
        if chart_type == 'VIDEOS':
            return [{'id': 'v1', 'title': 'Song', 'artists': [{'name': 'Artist'}],
                     'chartEntryMetadata': {'currentPosition': 1}, 'viewCount': 100}]
        raise RuntimeError('unavailable')
    monkeypatch.setattr(yt, '_browse', browse)
    def no_fallback(*args):
        pytest.fail('must retain weekly chart instead of replacing it with mostPopular')
    monkeypatch.setattr(yt, '_api_popular_music', no_fallback)
    records = []
    result = run_crawler.run_source('charts', 'youtube', yt.crawl_charts, None,
        SimpleNamespace(send=lambda *args: records.append(args)), 1)
    assert result['records'] == 1 and result['status'] == 'partial'
    assert records[0][2]['chart_id'] == 'youtube_top_videos_vn_weekly'
    assert records[0][2]['metric_name'] == 'weekly_views'


def test_all_weekly_endpoints_failed_use_explicit_fallback(monkeypatch):
    monkeypatch.setenv('YOUTUBE_API_KEY', 'test-only-key')
    def unavailable(*args):
        raise RuntimeError('unavailable')
    monkeypatch.setattr(yt, '_browse', unavailable)
    monkeypatch.setattr(yt, '_api_popular_music', lambda *args: ['fallback'])
    with common.capture_issues() as issues:
        assert yt.chart_videos(None) == ['fallback']
    assert {x['component'] for x in issues} >= {'youtube:TRACKS', 'youtube:VIDEOS'}


def test_ambiguous_artist_is_marked_unresolved_and_uploader_retained():
    class Response:
        status_code = 200
        def json(self):
            return {'items': [{'id': 'v1', 'snippet': {
                'title': 'Tìm Em - Hngle | Official MV', 'channelTitle': 'Vie Channel'},
                'statistics': {}, 'contentDetails': {}}]}
    track = yt._api_popular_music(SimpleNamespace(get=lambda *a, **k: Response()), 'key')[0][2]
    assert track['artist_resolution'] == 'unresolved'
    assert track['artists'] == [] and track['uploader_name'] == 'Vie Channel'
    assert track['track_key'].endswith('__unknown')


def test_explicit_collaboration_can_resolve_artist_first():
    title, artists = yt._api_title_artists('Hngle ft. Bảo Anh - Tìm Em | Official MV', 'Vie Channel')
    assert title == 'Tìm Em' and artists == ['Hngle', 'Bảo Anh']
