import ast
import importlib
import json
from datetime import date
from pathlib import Path
import unicodedata
from types import SimpleNamespace

from hypothesis import given, settings, strategies as st
import pytest

case = pytest.mark.qa_case


@case('N-CRAWL-001', 'A partially emitted failed market remains a failed market', priority='P1',
      expected='All other countries attempted, emitted records retained, final failure signalled',
      technique='generator/fault-injection')
def test_partial_market_failure(monkeypatch):
    import common
    monkeypatch.setenv('COUNTRY_MARKETS', 'vn,th')
    attempted, received = [], []
    def provider(session, now, country):
        attempted.append(country)
        yield 'charts', 'a', {'platform_id':country}
        if country == 'vn':
            raise OSError('connection dropped after first row')
    with pytest.raises(RuntimeError, match='vn'):
        for record in common.crawl_markets(None, 0, provider):
            received.append(record)
    assert attempted == ['vn', 'th']
    assert [r[2]['platform_id'] for r in received] == ['vn', 'th']


def kworb_html(streams='100', period='2026/10/01'):
    cells = ['1', '=', '<a href="../track/aaa.html">Song</a><a href="../artist/a.html">Artist</a>',
             '0', '0', '0', streams, '0', '0', '0', '1000']
    return '<span class="pagetitle">'+period+'</span><tbody><tr>'+''.join('<td>'+v+'</td>' for v in cells)+'</tr></tbody>'


@case('N-CRAWL-002', 'Negative provider counts never become valid stream measurements', priority='P0',
      expected='Reject invalid capture before emitting a negative measurement', technique='boundary/parser')
@pytest.mark.parametrize('bad', ['-1', '-999999'])
def test_kworb_negative_streams(bad):
    import src_spotify
    response = SimpleNamespace(text=kworb_html(bad), raise_for_status=lambda:None)
    session = SimpleNamespace(get=lambda *a, **k:response)
    with pytest.raises(ValueError):
        list(src_spotify.crawl_kworb_daily(session, 1790845200000, 'vn'))


@case('N-CRAWL-003', 'Source dates emitted by provider parsers must be calendar dates', priority='P1',
      expected='Invalid source period rejected or explicitly unknown', technique='boundary/calendar')
@pytest.mark.parametrize('period', ['2026/02/30', '2026/13/01', '2026/00/01'])
def test_kworb_invalid_source_period(period):
    import src_spotify
    response = SimpleNamespace(text=kworb_html(period=period), raise_for_status=lambda:None)
    session = SimpleNamespace(get=lambda *a, **k:response)
    try:
        rows = list(src_spotify.crawl_kworb_daily(session, 1790845200000, 'vn'))
    except ValueError:
        return
    for _, _, row in rows:
        if row['source_period'] is not None:
            date.fromisoformat(row['source_period'])


def chart(country, ids):
    return [dict(record_type='chart_entry', source='spotify', data_provider='spotify_kworb',
                 chart_id='daily_'+country, country_code=country, period_type='daily', source_period='2026-10-01',
                 snapshot_id=country, crawled_at='2026-10-01T12:00:00Z', platform_id=track,
                 title=track, artists=['Artist'], rank=i, chart_size=len(ids), metric_value=i*10)
            for i, track in enumerate(ids, 1)]


@case('N-NUM-001', 'Pairwise overlap and ranking obey independent set and ordinal formulas', priority='P0',
      expected='Intersection/union, reciprocal rank normalization, finite stream shares', technique='property/differential')
@pytest.mark.qa_property
@settings(max_examples=80, deadline=None, derandomize=True)
@given(st.lists(st.sampled_from(list('abcdefgh')), min_size=1, max_size=6, unique=True),
       st.lists(st.sampled_from(list('abcdefgh')), min_size=1, max_size=6, unique=True))
def test_independent_country_math(left, right):
    model = importlib.import_module('insights')
    n = min(len(left), len(right))
    rows = chart('VN', left) + chart('TH', right)
    result = model.compare_country_tastes(rows, ['VN','TH'], top_n=n)
    a, b = set(left[:n]), set(right[:n])
    assert result['overlap'][0]['jaccard'] == pytest.approx(len(a & b)/len(a | b))
    for item in result['items']:
        for country, ids in [('VN',left), ('TH',right)]:
            entry = item['countries'][country]
            if item['track_id'] not in ids:
                assert entry is None
            else:
                rank = ids.index(item['track_id'])+1
                assert entry['rank_score'] == pytest.approx(1-(rank-1)/len(ids))
                assert entry['chart_stream_share'] == pytest.approx(rank/sum(range(1,len(ids)+1)))
    assert model.compare_country_tastes(rows[::-1]*2, ['VN','TH'], top_n=n) == result


@case('N-ARCH-003', 'Archive line errors retain valid records and expose diagnostics', priority='P1',
      expected='One valid record and exactly one diagnostic for each malformed line', technique='filesystem/fuzz')
@pytest.mark.parametrize('bad', ['null', '[]', '42', '"text"', '{not json'])
def test_archive_bad_line_diagnostics(env, bad):
    (env.root/'out-test').mkdir()
    good = {'record_type':'chart_entry', 'crawled_at':'2026-10-01T12:00:00Z'}
    (env.root/'data.jsonl').write_text(json.dumps(good)+'\n'+bad+'\n', encoding='utf-8')
    rows, metadata = env.model.load_local()
    assert rows == [good]
    assert len(metadata['errors']) == 1


@case('N-SPARK-001', 'Only missing raw data is an empty bootstrap; corrupt or unreadable data fails closed',
      priority='P0', expected='Non-missing AnalysisException propagates; no empty frame published',
      technique='actual-function-AST/fault-injection')
@pytest.mark.parametrize('error_code', ['PATH_NOT_FOUND', 'UNABLE_TO_INFER_SCHEMA', 'CANNOT_READ_FILE_FOOTER', 'PERMISSION_DENIED'])
def test_raw_analysis_error_is_not_empty(error_code):
    path = Path(__file__).resolve().parents[2]/'spark/jobs_common.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'read_raw')
    class AnalysisError(Exception):
        def getErrorClass(self):
            return error_code
    def read(*args):
        raise AnalysisError('Injected raw dataset error')
    created = []
    spark = SimpleNamespace(read=SimpleNamespace(parquet=read),
                            createDataFrame=lambda *args:created.append(args) or 'empty')
    namespace = {'SCHEMAS':{'charts':SimpleNamespace(fields=[])}, 'LAKE':'file:///owned',
                 'AnalysisException':AnalysisError, 'T':SimpleNamespace(StructType=lambda x:x, StructField=lambda *a:a,
                   TimestampType=lambda:None), '_S':None}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), namespace)
    if error_code == 'PATH_NOT_FOUND':
        assert namespace['read_raw'](spark, 'charts') == 'empty'
        assert len(created) == 1
    else:
        with pytest.raises(AnalysisError):
            namespace['read_raw'](spark, 'charts')
        assert created == []


@case('N-ID-001', 'Canonical Unicode equivalents merge the same song across playlist sources', priority='P1',
      expected='One song with two distinct playlist memberships; different songs remain distinct',
      technique='Unicode/metamorphic/cross-platform')
def test_playlist_unicode_identity():
    model = importlib.import_module('insights')
    rows=[]
    for source,form in [('spotify','NFC'),('apple_music','NFD')]:
        for i in range(5):
            rows.append(dict(record_type='playlist_item', source=source, playlist_id=source,
                platform_id=source+str(i), title=unicodedata.normalize(form,'Đêm Mưa') if i==0 else source+str(i),
                artists=[unicodedata.normalize(form,'Mỹ Tâm')], crawled_at='2026-10-01T12:00:00Z',
                position=i+1, playlist_size=5, playlist_type='editorial'))
    items=model.playlist_ranking(rows)['items']
    matches=[r for r in items if unicodedata.normalize('NFC',r['title'])=='Đêm Mưa']
    assert len(matches)==1
    assert matches[0]['playlist_count']==2
    assert len(items)==9


@case('N-NUM-002', 'Country winner scores average sources per day and observed days independently', priority='P0',
      expected='Hand-calculated ordinal average and stable winner under duplication/permutation', technique='reference/metamorphic')
def test_active_country_winner_reference():
    model=importlib.import_module('insights')
    rows=[]
    # Day1: Spotify A>B, Apple B>A. Day2: only Spotify B>A.
    # A=((1+.5)/2+.5)/2=.625; B=((.5+1)/2+1)/2=.875.
    for day,source,ids in [('2026-10-01','spotify',['A','B']),('2026-10-01','apple_music',['B','A']),
                           ('2026-10-02','spotify',['B','A'])]:
        for r in chart('VN',ids):
            r.update(source=source,data_provider=source,source_period=day,crawled_at=day+'T12:00:00Z')
            rows.append(r)
    expected=model.country_winners(rows,['VN'],2,date(2026,10,2))
    assert expected['items'][0]['title']=='B'
    assert expected['items'][0]['score']==pytest.approx(.875)
    assert model.country_winners(rows[::-1]*2,['VN'],2,date(2026,10,2))==expected
