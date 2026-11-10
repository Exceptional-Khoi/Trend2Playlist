import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'serving'))
import insights as m


def chart(day, rank=1, track='x', country='VN', size=2, crawl=None):
    return dict(record_type='chart_entry', source='spotify', data_provider='spotify_kworb',
                chart_id='daily_'+country, country_code=country, period_type='daily', source_period=day,
                crawled_at=(crawl or day)+'T12:00:00+00:00', platform_id=track, title=track,
                rank=rank, chart_size=size, metric_value=100)


def test_repeated_crawls_not_source_periods():
    rows=[chart('2026-10-01',crawl=d) for d in ['2026-10-01','2026-10-02','2026-10-03']]
    result=m.analyze_track_lifecycle(rows)['items'][0]
    assert result['periods']==1 and result['status']=='insufficient_data'


def test_lifecycle_trend_and_safe_absence():
    rows=[chart('2026-10-0'+str(i),rank=5-i,size=5) for i in (1,2,3)]
    assert m.analyze_track_lifecycle(rows)['items'][0]['status']=='rising'
    rows.append(chart('2026-10-04',track='y',size=2))
    assert next(x for x in m.analyze_track_lifecycle(rows)['items'] if x['track_id']=='x')['status']=='rising'
    rows.append(chart('2026-10-04',track='z',rank=2,size=2))
    assert next(x for x in m.analyze_track_lifecycle(rows)['items'] if x['track_id']=='x')['status']=='left_chart'


def test_country_same_period_and_full_denominator():
    rows=[chart('2026-10-01',country=c,track=t,rank=i) for c in ['VN','TH'] for i,t in enumerate(['x','y'],1)]
    out=m.compare_country_tastes(rows,['VN','TH'],top_n=1)
    assert out['overlap'][0]['jaccard']==1
    assert out['items'][0]['countries']['VN']['chart_stream_share']==0.5
    rows[-1]['source_period']='2026-10-02'
    assert not m.compare_country_tastes(rows,['VN','TH'],top_n=1)['items']


def test_country_missing_is_null():
    rows=[chart('2026-10-01',country=c,track=c+str(i),rank=i) for c in ['VN','TH'] for i in [1,2]]
    out=m.compare_country_tastes(rows,['VN','TH'],top_n=1)
    assert out['overlap'][0]['jaccard']==0
    assert any(x['countries']['VN'] is None for x in out['items'])


def test_playlist_excludes_mirrors_and_missing_future():
    p=dict(record_type='playlist_item',playlist_id='p',playlist_type='editorial',source='spotify',
           platform_id='x',position=1,crawled_at='2026-10-01T12:00:00+00:00')
    rows=[p,dict(p,playlist_id='q',playlist_type='chart_mirror'),chart('2026-10-01',rank=2),chart('2026-10-02',rank=1)]
    out=m.analyze_playlist_exposure(rows)
    assert len(out['items'])==1
    assert out['items'][0]['placement_score']==1
    assert out['items'][0]['rank_improvement']==1
    assert out['excluded_chart_mirror']==1


def test_window_does_not_refresh_old_data():
    assert not m.analyze([chart('2026-10-01')],'lifecycle',days=7,as_of='2026-11-02')['items']


def test_real_archive_country_evidence():
    import pytest
    if not (m.ROOT / 'out/country-feasibility-20261007T154533Z/records/spotify_kworb_vn.jsonl').exists():
        pytest.skip('Optional local crawl archive is not checked into Git')
    rows,_=m.load_local()
    out = m.analyze(rows, 'countries', days=7, as_of='2026-10-07', countries=['VN','TH'])
    assert [item['country'] for item in out['items']] == ['TH','VN']
    assert all(item['title'] and item['url'] for item in out['items'])


def test_numeric_value_is_not_kafka_envelope():
    assert m.normalize({'value': 4, 'source': 'trends'})['value']==4


def test_api_filters_and_removed_routes():
    from fastapi.testclient import TestClient
    from api import app
    client=TestClient(app)
    for feature in ['lifecycle','playlists','countries']:
        r=client.get('/api/insights/'+feature+'?days=90&as_of=2026-11-02')
        assert r.status_code==200
        assert 'items' in r.json()
    assert client.get('/api/insights/countries?as_of=bad').status_code==400
    assert client.get('/api/insights/countries?days=0').status_code==422
    assert client.get('/api/insights/feedback').status_code==404
    assert client.post('/api/listen',json={}).status_code==404
    assert 'insights.js' in client.get('/').text


def test_playlist_memberships_across_days():
    base = dict(record_type='playlist_item', source='spotify', platform_id='a', title='Song',
                playlist_id='p', playlist_size=5, crawled_at='2026-10-01')
    rows = [base, dict(base, crawled_at='2026-10-02'), dict(base, playlist_id='q'),
            dict(base, playlist_id='small', playlist_size=4)]
    # The new product requirement includes small playlists as well as large ones.
    assert m.playlist_ranking(rows)['items'][0]['playlist_count'] == 3


def test_country_winner_averages_available_sources_and_days():
    rows = []
    for day, source, order in [('01', 'spotify', 'ab'), ('01', 'apple_music', 'ba'),
                               ('02', 'spotify', 'ba')]:
        for rank, title in enumerate(order, 1):
            rows.append(dict(record_type='chart_entry', chart_id=source, source=source,
                             country_code='VN', crawled_at='2026-10-'+day, title=title,
                             platform_id=source+title, artists=['Artist'], rank=rank, chart_size=2))
    winner, = m.analyze(rows, 'countries', 2, '2026-10-02')['items']
    assert (winner['country'], winner['title'], winner['score']) == ('VN', 'b', .875)
