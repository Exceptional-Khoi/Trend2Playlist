from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_manifests_parse_and_configmap_files_fit():
    resources = []
    for path in (ROOT / 'k8s').rglob('*.yaml'):
        for resource in yaml.safe_load_all(path.read_text(encoding='utf-8')):
            assert resource['apiVersion'] and resource['kind'] and resource['metadata']['name'], path
            resources.append(resource)
    assert len(resources) > 30
    for folder in ('crawler', 'spark', 'serving/static'):
        assert sum(p.stat().st_size for p in (ROOT / folder).iterdir() if p.is_file()) < 900000


def test_cloud_uses_pipeline_and_country_job():
    docs=list(yaml.safe_load_all((ROOT/'k8s/60-serving.yaml').read_text(encoding='utf-8')))
    deployment=next(d for d in docs if d['kind']=='Deployment')
    env=deployment['spec']['template']['spec']['containers'][0]['env']
    assert {'name':'INSIGHT_BACKEND','value':'elasticsearch'} in env
    assert '53-country-charts.yaml' in (ROOT/'scripts/deploy.sh').read_text(encoding='utf-8')
    assert 'job/crawl-countries-init' in (ROOT/'scripts/bootstrap-data.sh').read_text(encoding='utf-8')


def test_pipeline_api_paginates_and_releases_scroll(monkeypatch):
    import sys, json, httpx
    sys.path.insert(0,str(ROOT/'serving'))
    import api
    from fastapi.testclient import TestClient
    calls=[]
    def handle(request):
        calls.append((request.method,request.url.path))
        if request.method=='DELETE':
            return httpx.Response(200,json={'succeeded':True})
        if request.url.path=='/music-insight-raw/_search':
            record={'event_id':'a','source':'web','action':'play','user_id':'u','track_key':'t','ts_ms':1793491200000}
            return httpx.Response(200,json={'_scroll_id':'s','hits':{'hits':[{'_source':{'value':json.dumps(record)}}]}})
        return httpx.Response(200,json={'_scroll_id':'s','hits':{'hits':[]}})
    monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch')
    monkeypatch.setattr(api,'es',httpx.AsyncClient(transport=httpx.MockTransport(handle),base_url='http://es'))
    response=TestClient(api.app).get('/api/insights/feedback?as_of=2026-11-05&days=30')
    assert response.status_code==200
    assert response.json()['items'][0]['play']==1
    assert ('DELETE','/_search/scroll') in calls


def test_pipeline_failure_never_falls_back_to_archive(monkeypatch):
    import api, httpx
    from fastapi.testclient import TestClient
    monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch')
    monkeypatch.setattr(api,'es',httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(503)),base_url='http://es'))
    assert TestClient(api.app).get('/api/insights/feedback').status_code==503
