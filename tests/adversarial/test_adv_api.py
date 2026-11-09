import asyncio
import concurrent.futures
import json
from types import SimpleNamespace

import pytest

from .support import case, chart, event


def archive(env, records):
    env.monkeypatch.setattr(env.model, 'load_local', lambda: (records, {'mode':'archive','errors':[]}))


def es_transport(env, handler):
    client=env.httpx.AsyncClient(base_url='http://isolated-es.invalid',transport=env.httpx.MockTransport(handler))
    env.monkeypatch.setattr(env.api,'es',client)
    return client


@case('G01','Chart insight routes return typed empty results',technique='HTTP/partition',expected='HTTP 200, items list, archive metadata')
@pytest.mark.parametrize('feature',['lifecycle','playlists','countries'])
def test_empty_routes(api_env,feature):
    archive(api_env,[])
    r=api_env.client.get('/api/insights/'+feature+'?as_of=2026-10-01')
    assert r.status_code==200 and isinstance(r.json()['items'],list)
    assert r.json()['data']['mode']=='archive'


@case('G02','Query parameter boundaries are enforced before dependencies',technique='HTTP/boundary',expected='Invalid values return 400 or 422, valid boundaries 200')
@pytest.mark.parametrize('query,status',[
    ('days=0',422),('days=-1',422),('days=366',422),('days=1',200),('days=365',200),
    ('top_n=0',422),('top_n=201',422),('top_n=1',200),('top_n=200',200),
    ('countries=VN',200),('countries=VN,VN',200),('countries=VN,TH',200),
    ('countries=V1,TH',400),('countries=VN,TH,ID,US,JP,FR,DE,GB,AU,CA,NZ',200),
    ('as_of=bad',400),('as_of=2026-02-30',400),('as_of=2026-W01-1',400),
    ('as_of=20261001',400)])
def test_query_validation(api_env,query,status):
    archive(api_env,[])
    assert api_env.client.get('/api/insights/countries?'+query).status_code==status


@case('G03','ISO country codes must use Latin alphabet, not any Unicode letters',technique='HTTP/fuzz',expected='400 for non-ISO Unicode pairs',risk='Invalid countries silently look like no-data')
@pytest.mark.parametrize('countries',['日本,VN','éé,VN','ΩΩ,VN','XX,VN'])
def test_country_iso_values(api_env,countries):
    archive(api_env,[])
    assert api_env.client.get('/api/insights/countries',params={'countries':countries}).status_code==400


@case('G04','Malformed archived records produce controlled 422, never HTTP 500',technique='HTTP/fuzz',priority='P0',expected='422 on malformed timestamp or nested record')
@pytest.mark.parametrize('row',[{'ts_ms':float('inf'),'action':'play'}, {'crawled_at':'no-date','record_type':'chart_entry'}, {'ts_ms':-1e40,'action':'play'}])
def test_malformed_archive(api_env,row):
    archive(api_env,[row])
    assert api_env.client.get('/api/insights/lifecycle').status_code==422


@case('G05','Malformed JSONL input is reported without crashing the loader',technique='HTTP/filesystem',expected='Valid lines preserved; invalid lines counted; HTTP 200 or controlled 422')
@pytest.mark.parametrize('line',['null','[]','42','"text"','{bad json'])
def test_bad_jsonl_top_level(api_env,line):
    folder=api_env.root/'out';folder.mkdir()
    (folder/'music.events.jsonl').write_text(json.dumps(event())+'\n'+line+'\n',encoding='utf-8')
    result=api_env.client.get('/api/insights/lifecycle?as_of=2026-10-01')
    assert result.status_code in (200,422), result.text[:200]


@case('G06','Elasticsearch errors never fall back to archive',technique='HTTP/fault-injection',priority='P0',expected='503, no archive access')
@pytest.mark.parametrize('fault',[401,403,404,429,500,502,503,'timeout','reset'])
def test_es_fail_closed(api_env,fault):
    api_env.monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch')
    api_env.monkeypatch.setattr(api_env.model,'load_local',lambda:pytest.fail('archive fallback'))
    def handler(req):
        if fault=='timeout':raise api_env.httpx.ReadTimeout('injected')
        if fault=='reset':raise api_env.httpx.ConnectError('injected')
        return api_env.httpx.Response(fault,json={'error':'injected'})
    es_transport(api_env,handler)
    assert api_env.client.get('/api/insights/lifecycle').status_code==503


@case('G07','Malformed ES envelope/value yields a controlled data error and releases scroll',technique='HTTP/fault-injection',priority='P0',expected='422 or 503, scroll delete attempted')
@pytest.mark.parametrize('value',['{bad',None,[],{'nested':'object'},'null','[1,2]'])
def test_malformed_es_value(api_env,value):
    api_env.monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch')
    deleted=[]
    def handler(req):
        if req.method=='DELETE':deleted.append(req);return api_env.httpx.Response(200,json={})
        return api_env.httpx.Response(200,json={'_scroll_id':'s','hits':{'hits':[{'_source':{'value':value}}]}})
    es_transport(api_env,handler)
    r=api_env.client.get('/api/insights/lifecycle')
    assert r.status_code in (422,503),r.text[:200]
    assert deleted


@case('G08','Raw record limit has exact inclusive boundary and releases scroll',technique='HTTP/boundary/stress',priority='P0',
      inputs='199999 / 200000 / 200001 real JSON decoded records, pages of 1000',expected='200/200/422, DELETE scroll on every path')
@pytest.mark.parametrize('count,status',[(199999,200),(200000,200),(200001,422)])
def test_record_limit(api_env,count,status):
    api_env.monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch')
    cursor=[0];deleted=[]
    value=json.dumps(event())
    def handler(req):
        if req.method=='DELETE':deleted.append(req);return api_env.httpx.Response(200,json={})
        n=min(1000,count-cursor[0]);cursor[0]+=n
        return api_env.httpx.Response(200,json={'_scroll_id':'s','hits':{'hits':[{'_source':{'value':value}} for _ in range(n)]}})
    es_transport(api_env,handler)
    # Isolate pagination/limit from the separately tested analytics; HTTP still decodes every record.
    api_env.monkeypatch.setattr(api_env.model,'analyze',lambda records,*a,**k:{'items':[],'read':len(records)})
    result=api_env.client.get('/api/insights/lifecycle')
    assert result.status_code==status
    assert deleted
    if status==200:assert result.json()['read']==count


@case('G09','Readiness health fails when required ES is unreachable',technique='HTTP/fault-injection',priority='P0',expected='HTTP 503; pod must not be Ready',risk='Broken dependency advertised healthy')
def test_health_dependency_status(api_env):
    api_env.monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch')
    def handler(req):raise api_env.httpx.ConnectError('injected offline')
    es_transport(api_env,handler)
    assert api_env.client.get('/api/health').status_code==503


@case('G10','Unknown backend mode fails closed',technique='configuration/decision-table',expected='503 configuration error, no silent archive fallback')
def test_unknown_backend(api_env):
    api_env.monkeypatch.setenv('INSIGHT_BACKEND','elastcisearch')
    archive(api_env,[])
    assert api_env.client.get('/api/insights/lifecycle').status_code==503


@case('G11','Compatibility endpoints remain discoverable and return stable empty structures',technique='HTTP/route-inventory',expected='HTTP 200 under empty ES; genuine removed routes 404')
@pytest.mark.parametrize('path',[
    '/api/meta','/api/provinces','/api/trending?province=ha-noi','/api/mentions?province=ha-noi',
    '/api/national','/api/charts','/api/charts/example','/api/analytics/hourly',
    '/api/realtime/timeline','/api/realtime/streaming','/api/users','/api/search?q=hello',
    '/api/similar/x','/api/recommend/u'])
def test_endpoint_inventory(api_env,path):
    es_transport(api_env,lambda req:api_env.httpx.Response(200,json={'hits':{'hits':[]},'aggregations':{}}))
    async def missing(*args):return None
    api_env.monkeypatch.setattr(api_env.api,'get_doc',missing)
    assert api_env.client.get(path).status_code==200


@case('G12','Removed dashboard and path traversal routes cannot expose files',technique='HTTP/security',expected='404, no application file bytes')
@pytest.mark.parametrize('path',['/legacy','/static/app.js','/static/index.html','/static/%2e%2e/api.py','/static/%2e%2e/%2e%2e/.env','/static/%252e%252e/api.py'])
def test_removed_and_traversal(api_env,path):
    assert api_env.client.get(path).status_code==404


@case('G13','HTTP JSON never emits NaN/Infinity',technique='HTTP/fuzz',priority='P0',expected='Controlled 422 for non-finite data')
@pytest.mark.parametrize('bad',[float('nan'),float('inf'),-float('inf')])
def test_nonfinite_serialization(api_env,bad):
    from .support import full_chart
    rows=full_chart(['x','y'])+full_chart(['x','y'],'TH');rows[0]['metric_value']=bad
    archive(api_env,rows)
    r=api_env.client.get('/api/insights/countries?top_n=2&as_of=2026-10-01')
    assert r.status_code==422


@case('G14','Legacy numeric pagination limits reject negative sizes',technique='HTTP/boundary',expected='422, never negative size sent to ES')
@pytest.mark.parametrize('path',['/api/national?limit=-1','/api/users?limit=-1','/api/charts/x?limit=-1','/api/analytics/hourly?size=-1','/api/realtime/timeline?minutes=-1'])
def test_legacy_lower_bounds(api_env,path):
    es_transport(api_env,lambda req:api_env.httpx.Response(200,json={'hits':{'hits':[]},'aggregations':{}}))
    assert api_env.client.get(path).status_code==422
