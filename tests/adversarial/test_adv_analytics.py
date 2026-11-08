import copy
import json
import math
import random
from datetime import datetime, timezone

import pytest

from .support import case, chart, full_chart, playlist, event, canonical_json


@case('A01', 'Three observed ranks determine direction or stability', technique='state-transition',
      inputs='Ranks [8,5,2], [2,5,8], [4,6,5], [1,8,3], two periods', expected='rising/falling/stable/fluctuating/insufficient_data')
@pytest.mark.parametrize('ranks,state', [([8,5,2],'rising'),([2,5,8],'falling'),([4,6,5],'stable'),([1,8,3],'fluctuating'),([3,2],'insufficient_data')])
def test_lifecycle_states(model, ranks, state, saved_input):
    rows = saved_input([chart(f'2026-10-0{i}', r, size=10) for i,r in enumerate(ranks,1)])
    assert model.analyze_track_lifecycle(rows)['items'][0]['status'] == state


@case('A02', 'Absence is established only by a complete later chart', technique='decision-table', priority='P0',
      inputs='Track x disappears; later snapshot full, incomplete, duplicate rank, duplicate identity', expected='left_chart only with full valid chart', risk='False removal from chart')
@pytest.mark.parametrize('variant', ['full','missing','duplicate_rank','duplicate_id'])
def test_absence_requires_complete_chart(model, variant):
    rows = [chart('2026-10-01',2, size=3)]
    later = full_chart(['a','b','c'],day='2026-10-02')
    if variant=='missing': later.pop()
    if variant=='duplicate_rank':later[2]['rank']=2
    if variant=='duplicate_id':later[2]['platform_id']='b'
    out = model.analyze_track_lifecycle(rows+later)
    status = next(x['status'] for x in out['items'] if x['track_id']=='x')
    assert (status=='left_chart') == (variant=='full')


@case('A03', 'Repeated identical records do not invalidate a complete snapshot', technique='metamorphic', priority='P0',
      inputs='A complete later chart duplicated byte-for-byte', expected='Same removal status and observations after dedup', risk='Duplicates hide real chart departures')
def test_duplicate_complete_snapshot(model, saved_input):
    rows = [chart('2026-10-01',2)] + full_chart(['a','b'],day='2026-10-02')
    doubled = saved_input(rows[:1]+rows[1:]*2)
    assert next(r['status'] for r in model.analyze_track_lifecycle(doubled)['items'] if r['track_id']=='x') == 'left_chart'


@case('A04', 'A returning track is present, not left_chart', technique='state-transition', expected='Present at latest period')
def test_returning_track(model):
    rows=full_chart(['x','a'])+full_chart(['a','b'],day='2026-10-02')+full_chart(['x','a'],day='2026-10-03')
    assert next(r for r in model.analyze_track_lifecycle(rows)['items'] if r['track_id']=='x')['status']!='left_chart'


@case('A05', 'Latest source-period snapshot wins independently of arrival order', technique='metamorphic', expected='Newer snapshot chosen, period counted once')
def test_latest_snapshot_per_period(model):
    old=chart(crawled_at='2026-10-02T10:00:00Z',rank=8,snapshot_id='old',size=10)
    new=chart(crawled_at='2026-10-02T11:00:00Z',rank=2,snapshot_id='new',size=10)
    for rows in [[old,new],[new,old]]:
        result=model.analyze_track_lifecycle(rows)['items'][0]
        assert result['periods']==1 and result['best_rank']==2


@case('A06', 'Platform scope separates equal IDs and chart labels', technique='equivalence',
      inputs='Same provider/id/chart label but spotify and apple_music sources', expected='Two independently scoped tracks', risk='Cross-platform false entity merge')
def test_lifecycle_source_scope(model):
    a=chart(data_provider='shared_provider',chart_id='daily',source='spotify')
    b=chart(data_provider='shared_provider',chart_id='daily',source='apple_music')
    assert len(model.analyze_track_lifecycle([a,b])['items'])==2


@case('A07', 'Invalid ranks cannot become valid best_rank values', technique='boundary',
      expected='Reject ValueError or exclude invalid row; never rank <=0, bool, NaN or beyond size')
@pytest.mark.parametrize('rank',[0,-1,3,True,float('nan')])
def test_invalid_lifecycle_rank(model, rank, saved_input):
    rows=saved_input([chart(rank=rank,size=2)])
    try: result=model.analyze_track_lifecycle(rows)
    except ValueError:return
    assert result['items']==[], 'invalid rank was incorporated into lifecycle output'


@case('A08', 'Equivalent aware timestamps represent the same instant', technique='metamorphic', expected='Same UTC instant; unknown period not fabricated')
def test_timezones_and_unknown_period(model):
    assert model.timestamp({'event_time':'2026-10-01T07:00:00+07:00'})==model.timestamp({'event_time':'2026-10-01T00:00:00Z'})
    out=model.analyze_track_lifecycle([chart(source_period=None)])['items'][0]
    assert out['periods']==0 and out['status']=='insufficient_data'


@case('A09', 'Epoch zero is a valid timestamp, not missing', technique='boundary', inputs='ts_ms=0, crawled_ms=1000', expected='1970-01-01T00:00Z', risk='Truthiness drops epoch boundary')
def test_epoch_zero(model):
    assert model.timestamp({'ts_ms':0,'crawled_ms':1000})==datetime(1970,1,1,tzinfo=timezone.utc)


@case('B01', 'Playlist placement formula uses logarithmic positions', technique='differential',
      inputs='Independent playlists positions 1,2,8 with lags 1/3/7', expected='Sum 1/log2(position+1), rank_before-rank_after')
@pytest.mark.parametrize('lag,after_rank',[ (1,1),(3,4),(7,3)])
def test_playlist_formula(model, lag, after_rank):
    rows=[playlist('p'+str(pos),pos) for pos in [1,2,8]]
    rows += [chart(rank=3,size=5),chart(f'2026-10-{1+lag:02}',after_rank,size=5)]
    item=model.analyze_playlist_exposure(rows)['items'][0]
    assert item['placement_score']==pytest.approx(math.fsum(1/math.log2(pos+1) for pos in [1,2,8]))
    assert item['playlist_count']==3 and item['rank_improvement']==3-after_rank


@case('B02', 'Only verified editorial playlists enter exposure statistics', technique='decision-table', expected='Mirrors/unknown excluded; editorial counted')
@pytest.mark.parametrize('kind', ['editorial','chart_mirror','unknown',None])
def test_playlist_classification(model, kind):
    row=playlist(playlist_type=kind)
    out=model.analyze_playlist_exposure([row,chart(),chart('2026-10-02')])
    assert bool(out['items'])==(kind=='editorial')


@case('B03', 'Invalid positions are excluded', technique='boundary', expected='Zero editorial exposures for invalid position')
@pytest.mark.parametrize('position',[0,-1,None,'1',True])
def test_playlist_invalid_position(model, position):
    assert model.analyze_playlist_exposure([playlist(position=position)])['editorial_exposures']==0


@case('B04', 'Duplicate playlist rows do not multiply exposure', technique='metamorphic', expected='Same independent score and counts')
def test_playlist_dedup_and_genre(model):
    rows=[playlist(),playlist('p2',2),chart(),chart('2026-10-02')]
    base=model.analyze_playlist_exposure(rows,genre='pop')
    assert model.analyze_playlist_exposure(rows*3,genre='pop')['items']==base['items']
    assert model.analyze_playlist_exposure(rows,genre='jazz')['items']==[]


@case('B05', 'No future observation or cross-platform pairing is invented', technique='decision-table', expected='No observation for missing lag or another source')
@pytest.mark.parametrize('future_source',[None,'apple_music'])
def test_playlist_missing_or_different_platform(model, future_source):
    rows=[playlist(),chart()]
    if future_source:rows.append(chart('2026-10-02',source=future_source))
    assert model.analyze_playlist_exposure(rows)['items']==[]


@case('B06', 'Positions outside a declared playlist size cannot contribute', technique='boundary',
      inputs='position=5, playlist_size=2', expected='Rejected or excluded', risk='Out-of-range exposure')
def test_playlist_declared_size(model):
    try:out=model.analyze_playlist_exposure([playlist(position=5,playlist_size=2)])
    except ValueError:return
    assert out['editorial_exposures']==0


@case('C01', 'Jaccard, rank score and shares agree with an independent set oracle', technique='differential', priority='P0', expected='Exact set intersection/union; full-chart denominator')
@pytest.mark.parametrize('a,b,n', [(['a','b'],['a','b'],2),(['a','b'],['c','d'],2),(['a','b'],['b','c'],2),(['a','b'],['a','c'],1)])
def test_country_reference(model,a,b,n):
    out=model.compare_country_tastes(full_chart(a)+full_chart(b,'TH'),['VN','TH'],top_n=n)
    left,right=set(a[:n]),set(b[:n])
    assert out['overlap'][0]['jaccard']==pytest.approx(len(left&right)/len(left|right))
    for item in out['items']:
        for country,tracks in [('VN',a),('TH',b)]:
            hit=item['countries'][country]
            if item['track_id'] not in tracks:assert hit is None
            else:
                rank=tracks.index(item['track_id'])+1
                assert hit['rank_score']==pytest.approx((len(tracks)-rank+1)/len(tracks))
                assert hit['chart_stream_share']==pytest.approx(1/len(tracks))


@case('C02', 'Countries are compared only at a common complete daily period', technique='decision-table', expected='No result for missing period, weekly chart, duplicate or missing ranks')
@pytest.mark.parametrize('fault',['date','missing','duplicate_rank','duplicate_id','weekly'])
def test_country_invalid_chart(model,fault):
    a=full_chart(['a','b']);b=full_chart(['a','b'],'TH')
    if fault=='date':
        for r in b:r['source_period']='2026-10-02'
    if fault=='missing':b.pop()
    if fault=='duplicate_rank':b[1]['rank']=1
    if fault=='duplicate_id':b[1]['platform_id']='a'
    if fault=='weekly':
        for r in b:r['period_type']='weekly'
    assert model.compare_country_tastes(a+b,['VN','TH'],top_n=1)['items']==[]


@case('C03', 'Missing streams are not silently imputed as zero', technique='boundary', inputs='One stream=100, another stream missing', expected='Unknown share or explicit rejection', risk='Inflated share with incomplete denominator')
def test_missing_stream_denominator(model):
    a=full_chart(['a','b']);a[1]['metric_value']=None
    out=model.compare_country_tastes(a+full_chart(['a','b'],'TH'),['VN','TH'],top_n=2)
    assert all(x['countries']['VN']['chart_stream_share'] is None for x in out['items'])


@case('C04', 'Valid shares are finite and within [0,1]', technique='boundary', inputs='Negative, infinity or NaN stream metric', expected='Controlled rejection or no invalid share')
@pytest.mark.parametrize('metric',[-50,float('inf'),float('nan')])
def test_invalid_streams(model,metric):
    a=full_chart(['a','b']);a[0]['metric_value']=metric
    try:out=model.compare_country_tastes(a+full_chart(['a','b'],'TH'),['VN','TH'],top_n=2)
    except ValueError:return
    for row in out['items']:
        value=row['countries']['VN']['chart_stream_share']
        assert value is None or (math.isfinite(value) and 0<=value<=1)


@case('C05', 'Countries must use compatible chart families', technique='decision-table',
      inputs='VN daily track chart versus TH daily viral chart', expected='Refuse mixed chart families', risk='Invalid cross-market comparison')
def test_country_chart_family(model):
    rows=full_chart(['a','b'],chart_id='tracks_daily')+full_chart(['a','b'],'TH',chart_id='viral_daily')
    assert model.compare_country_tastes(rows,['VN','TH'],top_n=2)['items']==[]


@case('C06', 'Zero stream is distinct from missing; top N cannot exceed chart', technique='boundary', expected='Zero share preserved; oversize top N empty')
def test_country_zero_and_top_n(model):
    a=full_chart(['a','b']);a[0]['metric_value']=0
    rows=a+full_chart(['c','d'],'TH')
    out=model.compare_country_tastes(rows,['VN','TH'],top_n=2)
    assert next(x for x in out['items'] if x['track_id']=='a')['countries']['VN']['chart_stream_share']==0
    assert model.compare_country_tastes(rows,['VN','TH'],top_n=3)['items']==[]


@case('D01', 'Feedback rates count actions and distinct event IDs, not duration', technique='differential', priority='P0', expected='skip/(play+skip), null for like-only')
@pytest.mark.parametrize('actions',[['play'],['skip'],['like'],['play','skip','like'],['play','play','skip']])
def test_feedback_reference(model,actions):
    rows=[event(str(i),a) for i,a in enumerate(actions)]
    out=model.summarize_listening_feedback(rows*3)['items'][0]
    plays,skips=actions.count('play'),actions.count('skip')
    assert out['events']==len(actions)
    assert out['skip_rate']==(skips/(plays+skips) if plays+skips else None)


@case('D02', 'Source separation, missing IDs, grouping and VN midnight boundaries', technique='equivalence', expected='Only selected source counted, missing ID dropped, UTC+7 hour')
@pytest.mark.parametrize('group',['track','genre','province','hour'])
def test_feedback_groups_sources(model,group):
    rows=[event('1',ts_ms=1790875800000),event('2',source='simulator'),event(None),event('3',source=None)]
    out=model.summarize_listening_feedback(rows,source='web',group_by=group)
    assert len(out['items'])==1 and out['items'][0]['events']==1
    # Hour grouping floors the local timestamp; this is not a minute bucket.
    if group=='hour':assert out['items'][0]['group']=='2026-10-02 00:00'
    assert model.summarize_listening_feedback(rows,source='unknown')['items'][0]['events']==1


@case('D03', 'Duplicate IDs do not make semantic results depend on arrival order', technique='metamorphic',
      inputs='Conflicting duplicate same ID: web play vs simulator skip', expected='Deterministic resolution or explicit rejection', risk='Replay order changes source attribution')
def test_conflicting_duplicate_order(model):
    rows=[event('same','play','web'),event('same','skip','simulator')]
    assert model.summarize_listening_feedback(rows)==model.summarize_listening_feedback(rows[::-1])


@case('D04', 'Window excludes old/future records without rewriting timestamps', technique='boundary', expected='Inclusive lower and exclusive upper UTC boundaries')
def test_window_bounds(model):
    low=int(datetime(2026,10,1,tzinfo=timezone.utc).timestamp()*1000)
    high=low+86400000
    rows=[event(str(x),ts_ms=x) for x in [low-1,low,high-1,high,high+1]]
    assert [r['ts_ms'] for r in model.window(rows,1,'2026-10-01')]==[low,high-1]


@case('D05', 'A malformed nested JSON envelope is not accepted as a record', technique='fuzz', expected='Controlled validation error; normalized result is a dictionary')
@pytest.mark.parametrize('bad',[None,[],[{'value':1}],42,'text'])
def test_normalize_invalid_top_level(model,bad):
    with pytest.raises((ValueError,TypeError)):
        model.normalize(bad)
