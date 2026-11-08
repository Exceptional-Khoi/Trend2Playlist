"""Phase-2 regressions exploring neighboring high-risk branches."""
import concurrent.futures
import json
from pathlib import Path
import threading
from types import SimpleNamespace

import pytest

from .support import case, chart, full_chart, playlist, event, isolated_function
from .test_adv_api import archive, producer_double


@case('X01','A late partial recrawl does not justify departure or union with old full snapshot',technique='regression/state-transition',priority='P0',expected='No false left_chart; one source period per recrawl')
def test_latest_partial_recrawl(model):
    old=full_chart(['a','b'],day='2026-10-02',snapshot_id='old',crawled_at='2026-10-02T10:00:00Z')
    new=[chart('2026-10-02',1,'a',snapshot_id='new',crawled_at='2026-10-02T11:00:00Z')]
    rows=[chart()]+old+new
    for data in (rows,rows[::-1]):
        x=next(r for r in model.analyze_track_lifecycle(data)['items'] if r['track_id']=='x')
        assert x['status']=='insufficient_data' and x['periods']==1


@case('X02','Three country pairwise comparisons match independent set oracle',technique='differential',expected='Three pair scores at 0, 1/3, 1')
def test_three_countries(model):
    tracks={'VN':['a','b'],'TH':['b','c'],'JP':['a','b']}
    rows=[r for c,ids in tracks.items() for r in full_chart(ids,c)]
    out=model.compare_country_tastes(rows,list(tracks),top_n=2)
    assert len(out['overlap'])==3
    for pair in out['overlap']:
        a,b=map(lambda c:set(tracks[c]),pair['countries'])
        assert pair['jaccard']==pytest.approx(len(a&b)/len(a|b))


@case('X03','Malformed track metadata cannot crash even archive button requests',technique='HTTP/fuzz',expected='Controlled validation/dependency error or sanitized event, never 500')
@pytest.mark.parametrize('mode',['archive','elasticsearch'])
@pytest.mark.parametrize('duration',['not-a-number',float('inf'),{'seconds':200}])
def test_listen_corrupt_metadata(api_env,mode,duration):
    archive(api_env,[chart()]);api_env.monkeypatch.setenv('INSIGHT_BACKEND',mode)
    async def document(*a):return {'duration_s':duration,'title':'X'}
    api_env.monkeypatch.setattr(api_env.api,'get_doc',document);producer_double(api_env)
    response=api_env.client.post('/api/listen',json={'user_id':'u','province_code':'ha-noi','track_key':'x'})
    assert response.status_code in (200,400,422,503)


@case('X04','Real HTTP adapter retries selected status codes, not auth/not-found',technique='loopback HTTP/fault-injection',expected='3 attempts for 429/503, 1 for 401/403/404; no external calls',environment='stdlib loopback HTTP server; requests transport real')
@pytest.mark.parametrize('status,attempts',[(429,3),(503,3),(401,1),(403,1),(404,1)])
def test_real_local_retry(status,attempts,monkeypatch):
    from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
    import common
    import urllib3.util.retry
    calls=[]
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(1);self.send_response(status if len(calls)<3 else 200)
            self.send_header('Content-Length','2');self.end_headers();self.wfile.write(b'{}')
        def log_message(self,*args):pass
    monkeypatch.setattr(urllib3.util.retry.Retry,'get_backoff_time',lambda self:0)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    session=common.http_session()
    try:
        response=session.get('http://127.0.0.1:'+str(server.server_port),timeout=3)
        assert len(calls)==attempts
        assert response.status_code==(200 if attempts==3 else status)
    finally:
        session.close();server.shutdown();server.server_close();thread.join(3)


@case('X05','Concurrent ES replacement cleanup reproduces against actual local ES',technique='real ES/concurrency',priority='P0',expected='Newer run document survives stale cleanup',environment='owned disposable ES; actual helper, writer adapter uses HTTP, no Spark connector')
@pytest.mark.qa_integration
def test_real_es_replace_race(source_root,isolated_services):
    import requests,uuid
    url=isolated_services.get('es_url')
    if not url:pytest.skip('BLOCKED: local ES absent')
    index='qa-race-'+uuid.uuid4().hex;written=threading.Event();done=threading.Event()
    requests.put(url+'/'+index,json={'mappings':{'properties':{'run_id':{'type':'keyword'}}}},timeout=15).raise_for_status()
    def write(run,*args):
        requests.put(url+'/'+index+'/_doc/track?refresh=true',json={'run_id':run},timeout=15).raise_for_status()
        if run=='run1':written.set();assert done.wait(15)
    def request(method,path,body=None):
        response=requests.request(method,url+path,json=body,timeout=15);response.raise_for_status();return response.json()
    fn=isolated_function(source_root/'spark/jobs_common.py','es_replace',{'es_write':write,'ES_NODES':'owned-local','_es_request':request})
    def second():
        assert written.wait(15)
        try:fn('run2',index,'id','run2')
        finally:done.set()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            a=pool.submit(fn,'run1',index,'id','run1');b=pool.submit(second);a.result(40);b.result(40)
        r=requests.get(url+'/'+index+'/_doc/track',timeout=15)
        assert r.status_code==200 and r.json()['_source']['run_id']=='run2',r.text
    finally:requests.delete(url+'/'+index,timeout=15).raise_for_status()
