"""Hardening regressions added while fixing the campaign findings."""
import itertools
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from .support import case, chart, full_chart, event, isolated_function, load_file
from .test_adv_api import archive, es_transport, producer_double


@case('FX01','Rejected/unknown-size snapshot cannot prove chart departure',priority='P0',expected='No left_chart without a verified complete snapshot')
@pytest.mark.parametrize('fault',['invalid_rank','unknown_size','inconsistent_size'])
def test_invalid_snapshot_no_false_departure(model,fault):
    later=full_chart(['a','b'],day='2026-10-02')
    if fault=='invalid_rank':later[0]['rank']=0
    if fault=='unknown_size':
        for row in later:row['chart_size']=None
    if fault=='inconsistent_size':later[1]['chart_size']=3
    result=model.analyze_track_lifecycle([chart()]+later)
    assert next(row for row in result['items'] if row['track_id']=='x')['status']!='left_chart'


@case('FX02','Equal-time recrawl ties are deterministic under permutations',technique='metamorphic',expected='One source period, consistent result regardless of arrival order')
def test_snapshot_time_tie(model):
    rows=[chart(snapshot_id='a',rank=1),chart(snapshot_id='b',rank=2)]
    outputs=[model.analyze_track_lifecycle(list(p)) for p in itertools.permutations(rows)]
    assert outputs[0]==outputs[1] and outputs[0]['items'][0]['periods']==1


@case('FX03','Non-Latin title mixed with Latin cannot collide; Latin aliases stay compatible',technique='identity regression',expected='Distinct mixed-script keys; existing Latin key unchanged')
def test_unicode_identity_and_compatibility():
    from shared.textnorm import track_key
    assert track_key('夜 Song',['Artist'])!=track_key('青 Song',['Artist'])
    assert track_key('Đêm Mưa',['Đen'])=='dem-mua__den'


@case('FX04','Conflicting event IDs are quarantined while clean events remain',priority='P0',expected='Only clean event counted with conflict ID exposed')
def test_event_conflict_quarantine(model):
    rows=[event('bad'),event('bad','skip'),event('ok','like')]
    out=model.summarize_listening_feedback(rows)
    assert out['conflicting_event_ids']==['bad']
    assert out['source_counts']=={'web':1}
    assert out['items'][0]['events']==out['items'][0]['like']==1


@case('FX05','Liveness survives dependency failure while readiness fails',technique='HTTP/deployment regression',priority='P0',expected='live 200, readiness503 for ES red/error, 200 yellow')
@pytest.mark.parametrize('status,body,ready',[(200,{'status':'red'},503),(503,{},503),(200,{'status':'yellow'},200)])
def test_separate_liveness_readiness(api_env,status,body,ready,source_root):
    import yaml
    api_env.monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch')
    es_transport(api_env,lambda _:api_env.httpx.Response(status,json=body))
    assert api_env.client.get('/api/health').status_code==ready
    assert api_env.client.get('/api/live').status_code==200
    deployment=next(r for r in yaml.safe_load_all((source_root/'k8s/60-serving.yaml').read_text(encoding='utf-8')) if r and r['kind']=='Deployment')
    container=deployment['spec']['template']['spec']['containers'][0]
    assert container['livenessProbe']['httpGet']['path']=='/api/live'
    assert container['readinessProbe']['httpGet']['path']=='/api/health'


@case('FX06','chart_date has the same strict calendar contract as as_of',technique='HTTP/boundary',expected='400 invalid date format')
@pytest.mark.parametrize('date',['20261001','2026-W01-1','2026-02-30'])
def test_chart_date_strict(api_env,date):
    archive(api_env,[])
    assert api_env.client.get('/api/insights/countries',params={'chart_date':date}).status_code==400


@case('FX07','Metadata dependency outage cannot silently accept an unverified track',technique='HTTP/fault-injection',priority='P0',expected='503, no Kafka send')
def test_listen_lookup_unavailable(api_env):
    api_env.monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch')
    async def fail(*a):raise api_env.httpx.ConnectError('injected lookup outage')
    api_env.monkeypatch.setattr(api_env.api,'get_doc',fail)
    sent=producer_double(api_env)
    r=api_env.client.post('/api/listen',json={'user_id':'u','province_code':'ha-noi','track_key':'x'})
    assert r.status_code==503 and not sent


@case('FX08','Unflushed archive events cannot be acknowledged as durable',technique='HTTP/filesystem fault',priority='P0',expected='503 on fsync failure')
def test_append_flush_failure(api_env):
    archive(api_env,[chart()])
    def fail(*a):raise OSError('injected fsync')
    api_env.monkeypatch.setattr(api_env.model.os,'fsync',fail)
    assert api_env.client.post('/api/listen',json={'user_id':'u','province_code':'ha-noi','track_key':'x'}).status_code==503


@case('FX09','Archive append is serialized across separate OS processes',technique='filesystem/multiprocess concurrency',priority='P0',inputs='4 processes x 40 Unicode events; start gated through stdin',expected='160 complete lines with unique IDs; no lost event')
def test_multiprocess_archive_append(tmp_path,source_root):
    worker="""import sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
import insights
insights.ROOT=Path(sys.argv[2])
sys.stdin.read(1)
for i in range(40):
    insights.append_web_event({'event_id':sys.argv[3]+'-'+str(i),'title':'\\u0110\\u00eam '+chr(0x1f3b5)*500})
"""
    processes=[subprocess.Popen([sys.executable,'-c',worker,str(source_root/'serving'),str(tmp_path),str(i)],
        stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) for i in range(4)]
    try:
        for process in processes:
            process.stdin.write(b'1');process.stdin.close();process.stdin=None
        for process in processes:
            stdout,stderr=process.communicate(timeout=40)
            assert process.returncode==0,stderr.decode('utf-8',errors='replace')
        rows=[json.loads(line) for line in (tmp_path/'out/insight-web-events.jsonl').read_text(encoding='utf-8').splitlines()]
        assert len(rows)==len({r['event_id'] for r in rows})==160
        assert all(row['title'].endswith(chr(0x1f3b5)*500) for row in rows)
    finally:
        for process in processes:
            if process.poll() is None:process.kill();process.communicate()


@case('FX10','Safe ES cleanup still removes obsolete documents in sequential reruns',technique='real ES/replacement regression',priority='P0',expected='Stale ID removed, new generation retained',environment='owned disposable local ES; production helper with HTTP writer adapter')
@pytest.mark.qa_integration
def test_real_es_cleanup_still_removes_old(source_root,isolated_services):
    import requests,uuid
    url=isolated_services.get('es_url')
    if not url:pytest.skip('BLOCKED: local ES absent')
    index='qa-cleanup-'+uuid.uuid4().hex
    requests.put(url+'/'+index,json={'mappings':{'properties':{'run_id':{'type':'keyword'}}}},timeout=15).raise_for_status()
    def write(rows,index,id_col):
        for key,run in rows.items():
            requests.put(url+'/'+index+'/_doc/'+key+'?refresh=true',json={'run_id':run},timeout=15).raise_for_status()
    def request(method,path,body=None):
        r=requests.request(method,url+path,json=body,timeout=15);r.raise_for_status();return r.json()
    fn=isolated_function(source_root/'spark/jobs_common.py','es_replace',{'ES_NODES':'local','es_write':write,'_es_request':request})
    try:
        fn({'keep':'old','stale':'old'},index,'id','old')
        fn({'keep':'new','added':'new'},index,'id','new')
        r=requests.get(url+'/'+index+'/_search',timeout=15);r.raise_for_status()
        assert {h['_id']:h['_source']['run_id'] for h in r.json()['hits']['hits']}=={'keep':'new','added':'new'}
    finally:requests.delete(url+'/'+index,timeout=15).raise_for_status()


@case('FX11','Missing view remains a valid bootstrap case',expected='None for not-found, exceptions for IO/corruption tested elsewhere')
@pytest.mark.parametrize('spark_error',[False,True])
def test_missing_view_bootstrap(source_root,spark_error):
    class Missing(Exception):
        def getErrorClass(self):return 'PATH_NOT_FOUND'
    class Reader:
        def json(self,*a):raise Missing() if spark_error else FileNotFoundError('not yet created')
    fn=isolated_function(source_root/'spark/jobs_common.py','read_view',{'VIEWS':'/temporary'})
    assert fn(SimpleNamespace(read=Reader()),'catalog') is None


@case('FX12','Synthetic fallback data cannot enter legacy entity resolution',technique='filesystem/provenance regression',expected='No canonical tracks from synthetic samples')
def test_synthetic_excluded_from_resolution(source_root,tmp_path,monkeypatch):
    module=load_file(source_root/'pipeline/entity_resolution.py','qa_synthetic_entities')
    monkeypatch.setattr(module,'OUT',tmp_path)
    row={'track_title':'Song','artist':'Artist','track_key':'apple:x','synthetic':True}
    (tmp_path/'apple_music_chart_vn.jsonl').write_text(json.dumps(row)+'\n',encoding='utf-8')
    assert module.resolve_entities()==[]


@case('FX13','Preflight failure cannot cause a batch write or deletion',technique='ES/fault-injection',priority='P0',expected='Failure propagated before any write')
def test_es_preflight_failure_preserves_data(source_root):
    calls=[]
    def request(*a):raise OSError('read failed')
    fn=isolated_function(source_root/'spark/jobs_common.py','es_replace',{'ES_NODES':'local','es_write':lambda *a:calls.append(a),'_es_request':request})
    with pytest.raises(OSError):fn(object(),'idx','id','new')
    assert not calls


@case('FX14','Dropping an invalid extra row must not silently promote snapshot completeness',technique='snapshot fault regression',priority='P0',expected='Invalid snapshot cannot prove departure; caller input unchanged')
def test_invalid_extra_row_not_promoted(model):
    later=full_chart(['a','b'],day='2026-10-02')+[chart('2026-10-02',0,'bad')]
    rows=[chart()]+later;before=json.dumps(rows,sort_keys=True)
    result=model.analyze_track_lifecycle(rows)
    assert next(r for r in result['items'] if r['track_id']=='x')['status']!='left_chart'
    assert json.dumps(rows,sort_keys=True)==before


@case('FX15','Listen HTTP validates real ES catalog and produces real Kafka web events',technique='real ES+Kafka+HTTP integration',priority='P0',
      expected='Three unique IDs received, source web, no fabricated duration',environment='owned ES/Kafka; TestClient handlers and producer/get_doc actual; no Spark/HDFS E2E claim')
@pytest.mark.qa_integration
def test_real_listen_es_kafka(api_env,source_root,isolated_services):
    import requests,httpx,uuid
    from urllib.parse import quote
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient,NewTopic
    from fastapi.testclient import TestClient
    url=isolated_services.get('es_url');bootstrap=isolated_services.get('kafka_bootstrap')
    if not url or not bootstrap:pytest.skip('BLOCKED: owned ES and Kafka required')
    suffix=uuid.uuid4().hex;track='qa-'+suffix;topic='qa-listen-'+suffix
    admin=KafkaAdminClient(bootstrap_servers=bootstrap,request_timeout_ms=10000)
    admin.create_topics([NewTopic(topic,1,1)])
    consumer=KafkaConsumer(topic,bootstrap_servers=bootstrap,auto_offset_reset='earliest',enable_auto_commit=False,
        group_id='qa-'+suffix,consumer_timeout_ms=10000,value_deserializer=json.loads)
    requests.put(url+'/music-tracks/_doc/'+track+'?refresh=true',json={'title':'QA','artists':['Artist'],'duration_s':200},timeout=15).raise_for_status()
    es=httpx.AsyncClient(base_url=url,timeout=15)
    api_env.monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch')
    for name,value in [('es',es),('ES_URL',url),('KAFKA_BOOTSTRAP',bootstrap),('TOPIC_EVENTS',topic)]:
        api_env.monkeypatch.setattr(api_env.api,name,value)
    get_doc=isolated_function(source_root/'serving/api.py','get_doc',{'es':es,'quote':quote})
    api_env.monkeypatch.setattr(api_env.api,'get_doc',get_doc)
    try:
        with TestClient(api_env.api.app) as client:
            responses=[client.post('/api/listen',json={'user_id':'qa-user','province_code':'ha-noi','track_key':track,'action':action})
                       for action in ['play','skip','like']]
            assert [r.status_code for r in responses]==[200]*3
            actual=[row.value for row in consumer]
            assert [e['action'] for e in actual]==['play','skip','like']
            assert {e['event_id'] for e in actual}=={r.json()['event']['event_id'] for r in responses}
            assert all(e['source']=='web' and e['listen_ms'] is None and e['duration_ms'] is None for e in actual)
    finally:
        if api_env.api._producer is not None:api_env.api._producer.close(timeout=10)
        consumer.close();admin.delete_topics([topic]);admin.close()
        requests.delete(url+'/music-tracks/_doc/'+track+'?refresh=true',timeout=15).raise_for_status()


@case('FX16','Legacy representative freshness compares instants across timezones',technique='entity/timezone regression',expected='Later UTC timestamp wins before rank tie-break')
def test_legacy_freshness_timezones(source_root,tmp_path,monkeypatch):
    module=load_file(source_root/'pipeline/entity_resolution.py','qa_entity_freshness')
    monkeypatch.setattr(module,'OUT',tmp_path)
    rows=[{'track_title':'Song','artist':'Artist','track_key':'spotify_chart:one','event_time':'2026-10-02T00:30:00+07:00','metrics':{'rank':2}},
          {'track_title':'Song','artist':'Artist','track_key':'spotify_chart:two','event_time':'2026-10-01T18:00:00Z','metrics':{'rank':3}}]
    (tmp_path/'spotify_chart_VN.jsonl').write_text('\n'.join(json.dumps(r) for r in rows),encoding='utf-8')
    result=module.resolve_entities()[0]
    assert result['ids']['spotify']=='two' and result['cross_platform_ranks']['spotify']==3


@case('FX17','Lifecycle output identifies the platform separately from its provider',technique='schema/source regression',expected='Spotify and Apple visible as distinct sources with shared provider')
def test_lifecycle_platform_provenance(model):
    rows=[chart(source=source,data_provider='syndicator',chart_id='daily') for source in ['spotify','apple_music']]
    result=model.analyze_track_lifecycle(rows)['items']
    assert {r['source'] for r in result}=={'spotify','apple_music'}
    assert all(r['data_provider']=='syndicator' for r in result)


@case('FX18','Country comparison rejects contradictory platform/provider metadata',technique='schema/source regression',expected='No Spotify comparison using an Apple chart')
def test_country_platform_metadata(model):
    rows=full_chart(['a','b'])+full_chart(['a','b'],'TH',source='apple_music')
    assert model.compare_country_tastes(rows,['VN','TH'],top_n=2)['items']==[]
