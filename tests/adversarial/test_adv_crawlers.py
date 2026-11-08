import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from .support import case, Response, Session, kworb_html, load_file, isolated_function


@case('E01','Every source refuses HTTP errors without emitting fabricated records',technique='mocked HTTP/fault-injection',priority='P0',
      inputs='Apple/Spotify/Zing/YouTube/Trends × 401/403/404/429/500/502/503/timeout/reset/DNS',expected='Exception or empty output, never data',environment='mocked HTTP only')
@pytest.mark.parametrize('source',['apple','spotify','zing','youtube','trends'])
@pytest.mark.parametrize('fault',[401,403,404,429,500,502,503,'timeout','reset','dns'])
def test_source_http_faults(monkeypatch,source,fault):
    if isinstance(fault,int):value=Response({},status=fault)
    else:value={'timeout':requests.Timeout('injected'),'reset':requests.ConnectionError('reset'),'dns':requests.ConnectionError('DNS')}[fault]
    session=Session(value)
    try:
        if source=='apple':
            import src_apple
            rows=list(src_apple.crawl_charts(session,1790812800000))
        elif source=='spotify':
            import src_spotify
            rows=list(src_spotify.crawl_kworb_daily(session,1790812800000,'vn'))
        elif source=='zing':
            import src_zing
            client=src_zing.ZingClient(session);client._cookie_ready=True
            rows=[client.get('/api/test')]
        elif source=='youtube':
            import src_youtube
            rows=src_youtube._api_popular_music(session,'not-a-real-key')
        else:
            import src_trends
            client=object.__new__(src_trends.TrendsClient);client.s=session
            monkeypatch.setattr(src_trends.time,'sleep',lambda _:None)
            monkeypatch.setattr(client,'_cookie',lambda:None)
            rows=[client._json('https://no-network.invalid',{})]
    except (requests.RequestException,ValueError,KeyError,TypeError,RuntimeError):return
    assert not rows


@case('E02','Malformed successful responses never produce records with fabricated schema',technique='mocked HTTP/fuzz',expected='Empty result or explicit parse failure')
@pytest.mark.parametrize('source',['apple','spotify','zing','youtube','trends'])
@pytest.mark.parametrize('payload',[None,[],{}, {'feed':{'results':None}}])
def test_malformed_200(monkeypatch,source,payload):
    session=Session(Response(payload,text=''))
    try:
        if source=='apple':
            import src_apple
            rows=list(src_apple.crawl_charts(session,1000))
        elif source=='spotify':
            import src_spotify
            rows=list(src_spotify.crawl_kworb_daily(session,1000))
        elif source=='zing':
            import src_zing
            z=src_zing.ZingClient(session);z._cookie_ready=True;rows=[z.get('/api/test')]
        elif source=='youtube':
            import src_youtube
            rows=src_youtube._api_popular_music(session,'fake')
        else:
            import src_trends
            t=object.__new__(src_trends.TrendsClient);t.s=session;rows=[t._json('unused',{})]
    # These source helpers are below the runner's documented Exception boundary.
    # AttributeError is also a parse refusal, not evidence that bad data was emitted.
    except (ValueError,KeyError,TypeError,AttributeError,RuntimeError,requests.RequestException):return
    assert not rows


@case('E03','Apple mapper preserves Unicode, source ID and ranking',technique='schema/partition',expected='Exact input title/artist/ID; ranks 1..N')
def test_apple_valid_unicode():
    import src_apple
    response=Response({'feed':{'results':[{'id':'1','name':'Đêm 🎵','artistName':'Đen','genres':[]}]}})
    rows=list(src_apple.crawl_charts(Session(response),1000))
    assert len(rows)==1
    r=rows[0][2]
    assert (r['title'],r['artists'],r['platform_id'],r['rank'])==('Đêm 🎵',['Đen'],'1',1)


@case('E04','A source with missing title cannot emit a usable chart record',technique='schema/boundary',expected='Reject or skip record; no unknown identity chart')
@pytest.mark.parametrize('bad',[None,'','   '])
def test_missing_titles(bad):
    import src_apple
    response=Response({'feed':{'results':[{'id':'1','name':bad,'artistName':'A'}]}})
    try:rows=list(src_apple.crawl_charts(Session(response),1000))
    except ValueError:return
    assert not rows


@case('E05','Kworb HTML parser preserves period, country and IDs',technique='mocked HTML/schema',expected='Daily period from source heading, no crawl-date substitution')
def test_kworb_fixture():
    import src_spotify
    rows=list(src_spotify.crawl_kworb_daily(Session(Response({},text=kworb_html())),1790899200000,'th'))
    assert len(rows)==2
    assert [r[2]['platform_id'] for r in rows]==['aaa','bbb']
    assert all(r[2]['source_period']=='2026-10-01' and r[2]['country_code']=='TH' for r in rows)


@case('E06','Changed HTML cannot silently parse as a valid chart',technique='mocked HTML/fuzz',expected='No records when table layout changed')
@pytest.mark.parametrize('html',['','<html>maintenance</html>','<table><tr><td>song</td></tr></table>'])
def test_kworb_changed_layout(html):
    import src_spotify
    assert list(src_spotify.crawl_kworb_daily(Session(Response({},text=html)),1000))==[]


@case('E07','Duplicate IDs and nonpositive ranks are not published as clean country charts',technique='data-quality',
      inputs='HTML duplicate track ID or rank 0/-1',expected='Reject/skip malformed chart',risk='Invalid source data reported as successful countries crawl')
@pytest.mark.parametrize('ids,ranks',[(('a','a'),[1,2]),(('a','b'),[0,1]),(('a','b'),[-1,1])])
def test_kworb_invalid_chart(ids,ranks):
    import src_spotify
    try:rows=list(src_spotify.crawl_kworb_daily(Session(Response({},text=kworb_html(ids,ranks))),1000))
    except ValueError:return
    assert not rows


@case('E08','A failing source does not discard independent successful sources',technique='fault-injection/CLI',priority='P0',expected='Partial mode preserves good records; strict mode exits failure')
@pytest.mark.parametrize('strict',[False,True])
def test_source_failure_isolation(monkeypatch,strict):
    import run_crawler as run
    class Sink:
        count=0;errors=0
        def send(self,*args):self.count+=1
        def close(self):pass
    sink=Sink()
    def good(*args,**kwargs):yield 'music.charts','id',{'title':'valid'}
    def bad(*args,**kwargs):raise requests.Timeout('injected');yield
    monkeypatch.setattr(run,'JOBS',{'charts':[('bad',bad),('good',good)]})
    monkeypatch.setattr(run,'make_sink',lambda *a,**k:sink)
    monkeypatch.setattr(run,'http_session',lambda:object())
    monkeypatch.setattr(run,'setup_logging',lambda:None)
    monkeypatch.setattr('sys.argv',['run_crawler','--job','charts'])
    monkeypatch.setenv('FAIL_ON_PARTIAL',str(strict).lower())
    if strict:
        with pytest.raises(SystemExit) as e:run.main()
        assert e.value.code==1
    else:run.main()
    assert sink.count==1


@case('E09','Repeated pagination rows are deduplicated by comment ID',technique='pagination/metamorphic',expected='Each comment emitted once; bounded calls')
def test_comment_repeated_page():
    import src_youtube
    comment={'id':'one','snippet':{'topLevelComment':{'snippet':{'publishedAt':'2026-10-01T12:00:00Z','textOriginal':'X'}}}}
    response=Response({'items':[comment],'nextPageToken':'same'})
    session=Session(response)
    rows=list(src_youtube._api_comments(session,'fake','video',4,None))
    assert len({r['comment_id'] for r in rows})==len(rows)


@case('E10','429 backoff is bounded and honors increasing delays without real waiting',technique='fault-injection/state-transition',expected='Four attempts, delays 60/120/180/240, then RuntimeError')
def test_trends_backoff(monkeypatch):
    import src_trends
    t=object.__new__(src_trends.TrendsClient);t.s=Session(Response({},status=429))
    delays=[];monkeypatch.setattr(src_trends.time,'sleep',delays.append)
    monkeypatch.setattr(src_trends.random,'uniform',lambda a,b:0)
    monkeypatch.setattr(t,'_cookie',lambda:None)
    with pytest.raises(RuntimeError):t._json('unused',{})
    assert delays==[60,120,180,240] and len(t.s.calls)==4


@case('E11','Kafka sink reports asynchronous delivery failures',technique='mocked producer/fault-injection',expected='Count error and flush/close; never claim transport E2E')
def test_sink_async_failure(monkeypatch):
    import common,kafka
    class Producer:
        def __init__(self,**kwargs):self.config=kwargs
        def send(self,*args,**kwargs):return SimpleNamespace(add_errback=lambda cb:cb(RuntimeError('failed')))
        def flush(self,**kwargs):pass
        def close(self,**kwargs):pass
    monkeypatch.setattr(kafka,'KafkaProducer',Producer)
    sink=common.KafkaSink('not-used:9092');sink.send('t','k',{'value':1});sink.close()
    assert sink.count==1 and sink.errors==1
    assert sink.producer.config['acks']=='all'


@case('E12','JSON serialization must not publish non-standard NaN',technique='schema/fuzz',expected='ValueError or no non-RFC JSON',risk='NaN payload crosses Kafka boundary')
def test_jsonl_nonfinite(tmp_path):
    import common
    sink=common.JsonlSink(str(tmp_path/'sink'))
    try:
        try:sink.send('music.events','k',{'metric':float('nan')})
        except ValueError:return
    finally:sink.close()
    text=(tmp_path/'sink/music.events.jsonl').read_text(encoding='utf-8')
    json.loads(text,parse_constant=lambda token:(_ for _ in ()).throw(AssertionError('Non-RFC '+token)))


@case('J01','JSONL topic cannot escape its output directory',technique='security/path-traversal',expected='Reject topic traversal, no outside file',environment='temporary filesystem')
def test_jsonl_path_traversal(tmp_path):
    import common
    sink=common.JsonlSink(str(tmp_path/'safe'))
    try:
        try:sink.send('../outside','k',{'x':1})
        except (ValueError,OSError):pass
    finally:sink.close()
    assert not (tmp_path/'outside.jsonl').exists()


@case('E13','Legacy fallback records must identify themselves as synthetic',technique='legacy/fault-injection',priority='P1',
      inputs='Legacy Apple CLI network failure with no API key',expected='Synthetic provenance or nonzero failure; never ordinary apple_music records')
def test_legacy_fallback_provenance(source_root,monkeypatch,tmp_path):
    import argparse,os,requests
    import datetime,hashlib
    legacy=load_file(source_root/'crawlers/common.py','qa_legacy_common')
    target=source_root/'crawlers/apple_music_charts.py';captured=[]
    base={'base_record':legacy.base_record}
    fallback=isolated_function(target,'get_fallback_sample',base)
    ns={'argparse':argparse,'os':os,'requests':requests,'load_dotenv':lambda:None,
        'fetch_rss_charts':lambda *a:(_ for _ in ()).throw(requests.Timeout('injected')),
        'get_fallback_sample':fallback,'write_jsonl':lambda name,rows:captured.extend(rows)}
    monkeypatch.delenv('APPLE_MUSIC_DEVELOPER_TOKEN',raising=False)
    monkeypatch.setattr('sys.argv',['apple','--storefront','vn'])
    isolated_function(target,'main',ns)()
    assert not captured or all(r.get('synthetic') or r.get('provenance')=='synthetic' for r in captured)
