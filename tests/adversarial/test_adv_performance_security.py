import gc
import json
import os
from pathlib import Path
import time
import tracemalloc

import pytest

from .support import case, chart, full_chart, event


@case('J03','Measured analysis scaling preserves valid results',technique='performance/stress',
      inputs='64/256/1024 distinct tracks, 3 periods; exact counts',expected='All unique tracks retained; record measured time/memory without invented SLA',environment='bounded local CPU/RAM')
@pytest.mark.qa_performance
@pytest.mark.parametrize('count',[64,256,1024])
def test_analysis_scaling(model,count,tmp_path):
    rows=[]
    for day in ['2026-10-01','2026-10-02','2026-10-03']:
        rows.extend(full_chart([str(i) for i in range(count)],day=day))
    gc.collect();tracemalloc.start();start=time.perf_counter();cpu=time.process_time()
    out=model.analyze_track_lifecycle(rows)
    elapsed=time.perf_counter()-start;cpu=time.process_time()-cpu;_,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
    assert len(out['items'])==count
    report={'case':'J03','records':len(rows),'tracks':count,'elapsed_seconds':elapsed,
            'cpu_seconds':cpu,'records_per_second':len(rows)/elapsed,'tracemalloc_peak_bytes':peak}
    path=Path(os.getenv('QA_REPORT_DIR') or str(tmp_path))/'performance.jsonl'
    with path.open('a',encoding='utf-8') as f:f.write(json.dumps(report)+'\n')


@case('J05','Repeated analyses remain semantically stable and do not mutate inputs',technique='repeatability/metamorphic',expected='Same output for 20 invocations; input unchanged')
def test_repeated_analysis(model):
    rows=full_chart(['a','b'])+full_chart(['b','c'],'TH')
    before=json.dumps(rows,sort_keys=True);expected=model.compare_country_tastes(rows,['VN','TH'],top_n=2)
    for _ in range(20):assert model.compare_country_tastes(rows,['VN','TH'],top_n=2)==expected
    assert json.dumps(rows,sort_keys=True)==before


@case('J06','Read-only archive access errors do not become unexplained HTTP 500',technique='HTTP/filesystem-fault',expected='Controlled 503 for PermissionError',risk='Unreadable archive crashes whole endpoint')
def test_archive_permissions(api_env):
    def denied():raise PermissionError('injected archive denied')
    api_env.monkeypatch.setattr(api_env.model,'load_local',denied)
    assert api_env.client.get('/api/insights/lifecycle').status_code==503


@case('J07','Extremely long untrusted names remain literal JSON data',technique='HTTP/security/stress',expected='No execution or corrupt response')
def test_long_untrusted_title(api_env):
    text='<img src=x onerror="window.__qa_xss=1">'+'🎵'*10000
    api_env.monkeypatch.setattr(api_env.model,'load_local',lambda:([chart(title=text)],{'mode':'archive'}))
    r=api_env.client.get('/api/insights/lifecycle?as_of=2026-10-01')
    assert r.status_code==200 and r.json()['items'][0]['title']==text


@case('J08','Untrusted titles render as text, not executable HTML',technique='real browser DOM/security',expected='No script execution; literal attack text visible',environment='Edge/Playwright; all HTTP routes fulfilled in memory, not service E2E')
@pytest.mark.qa_browser
def test_dashboard_xss(source_root):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:pytest.skip('BLOCKED: Playwright unavailable')
    edge=Path(r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe')
    if not edge.exists():pytest.skip('BLOCKED: local Edge unavailable')
    attack='<img src=x onerror="window.__qa_xss=1">'
    payload={'items':[{'track_id':'x','title':attack,'source':'qa','chart_id':'qa','periods':1,
                       'best_rank':1,'status':'insufficient_data','reason':'qa','first_observed':'2026-10-01',
                       'observations':1,'history':[]}], 'input_records':1,'note':'qa','as_of':'2026-10-01',
             'data':{'mode':'archive','note':'offline browser test'}}
    with sync_playwright() as p:
        browser=p.chromium.launch(executable_path=str(edge),headless=True)
        page=browser.new_page()
        def route(r):
            path=r.request.url.split('qa.invalid',1)[-1]
            if path.startswith('/api/insights/'):
                r.fulfill(status=200,content_type='application/json',body=json.dumps(payload))
            elif path.startswith('/static/'):
                file=source_root/'serving/static'/path.rsplit('/',1)[-1]
                r.fulfill(status=200,body=file.read_bytes(),content_type='text/javascript' if file.suffix=='.js' else 'text/css')
            elif path=='/':r.fulfill(status=200,content_type='text/html',body=(source_root/'serving/static/insights.html').read_text(encoding='utf-8'))
            else:r.abort()
        page.route('**/*',route)
        page.goto('http://qa.invalid/',wait_until='domcontentloaded')
        page.wait_for_function("document.querySelectorAll('tbody tr').length===1")
        assert page.evaluate('window.__qa_xss') is None
        assert page.locator('tbody').inner_text().find(attack)>=0
        browser.close()
