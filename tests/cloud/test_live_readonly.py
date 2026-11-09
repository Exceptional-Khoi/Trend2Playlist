"""Real GKE observations; intentionally not called fresh-data E2E or dev validation."""
import json
import datetime
import re
import urllib.parse

import pytest

from .cases import case


@pytest.fixture
def live(request):
    observer=getattr(request.config,'cloud_readonly',None)
    if observer is None:pytest.skip('BLOCKED: explicit read-only GKE runner required')
    observer.current=request.node.get_closest_marker('cloud_case').kwargs['id']
    return observer


@case('RO-001','Actual deployed API readiness agrees with real ES cluster',1,kind='integration',services='API,Elasticsearch',timeout=60)
def test_live_api_es_health(live):
    es=live.read('/_cluster/health','elasticsearch');api=live.read('/api/health')
    assert es['status']==200 and es['body']['status'] in ('green','yellow')
    assert api['status']==200 and api['body']['elasticsearch']==es['body']['status']


@case('RO-002','Existing Kafka offset bytes agree with ES raw document',2,kind='integration',services='Kafka,Elasticsearch',timeout=90)
def test_live_kafka_es_identity(live):
    # Prefer later physical documents in this bounded sample. An unsorted first
    # page repeatedly selects an offset already expired from Kafka retention.
    # Physical order is only sampling; exact topic/partition/offset bytes remain
    # the oracle, and retention misses remain INCONCLUSIVE.
    result=live.read('/music-insight-raw/_search','elasticsearch',
        {'size':100,'sort':[{'_doc':'desc'}],'query':{'match_all':{}}})
    assert result['status']==200,result
    candidates=[h for h in result['body']['hits']['hits'] if re.fullmatch(r'music\.[a-z-]+:\d+:\d+',h['_id'])]
    if not candidates:pytest.skip('BLOCKED: no offset-addressed raw records in bounded sample')
    candidate=candidates[0];topic,partition,offset=candidate['_id'].rsplit(':',2)
    actual=live.kafka_record(topic,int(partition),int(offset))
    if actual is None:pytest.skip('INCONCLUSIVE: Kafka retention no longer contains sampled raw offset')
    assert actual['value']==candidate['_source']['value'],'ES raw payload differs from retained Kafka offset'
    live.trace('partial_pipeline','PASS',{'document_id':candidate['_id'],'offset':offset,
        'note':'Existing deployed source; proves Kafka↔ES identity only, not fresh dev E2E or HDFS'})


@case('RO-003','Actual chart REST output agrees with actual ES current ranking',3,kind='integration',services='API,Elasticsearch',timeout=90)
def test_live_chart_read_consistency(live):
    result=live.read('/music-rt-charts/_search','elasticsearch',{'size':1,'query':{'match_all':{}}})
    assert result['status']==200,result
    rows=result['body']['hits']['hits']
    if not rows:pytest.skip('BLOCKED: no realtime charts')
    chart=rows[0]['_source']['chart_id']
    query={'size':1,'query':{'term':{'chart_id':chart}},'sort':[{'rank':'asc'}]}
    before=live.read('/music-rt-charts/_search','elasticsearch',query)
    if not before['body']['hits']['hits']:pytest.skip('INCONCLUSIVE: chart mapping requires a different exact field')
    api=live.read('/api/charts/'+urllib.parse.quote(chart,safe='')+'?limit=1')
    after=live.read('/music-rt-charts/_search','elasticsearch',query)
    if before['body']['hits']['hits']!=after['body']['hits']['hits']:pytest.skip('INCONCLUSIVE: source chart updated during bounded read')
    assert api['status']==200
    assert api['body']['items']==[h['_source'] for h in before['body']['hits']['hits']]


@case('RO-004','Four deployed dashboard insight endpoints remain readable',4,kind='integration',services='API,Elasticsearch',timeout=120)
def test_live_four_insights_readable(live):
    results={f:live.read('/api/insights/'+f+'?days=1&top_n=1') for f in ('lifecycle','playlists','countries')}
    assert live.read('/api/comments/tracks?days=1')['status']==200
    live.trace('deployed_feature_responses','OBSERVED',results)
    assert all(r['status']==200 for r in results.values()),{f:r['status'] for f,r in results.items()}
    assert all(r['body']['data']['mode']=='elasticsearch' for r in results.values())


@case('RO-005','Readiness cannot hide repeated recent worker OOM',5,kind='integration',services='Kubernetes,Spark',timeout=45)
def test_live_worker_stability_precondition(live):
    pods=json.loads(live.kube('get','pods','-n',live.live_namespace,'-l','app=spark-worker','-o','json'))['items']
    now=datetime.datetime.now(datetime.timezone.utc);unstable=[]
    for pod in pods:
        for status in pod['status'].get('containerStatuses',[]):
            last=status.get('lastState',{}).get('terminated',{})
            if last.get('reason')!='OOMKilled' or status.get('restartCount',0)<3:continue
            finished=datetime.datetime.fromisoformat(last['finishedAt'].replace('Z','+00:00'))
            if (now-finished).total_seconds()<=1800:
                unstable.append({'pod':pod['metadata']['name'],'ready':status.get('ready'),
                    'restartCount':status['restartCount'],'lastTermination':last,
                    'resources':[c.get('resources') for c in pod['spec']['containers']],
                    'safe_worker_settings':[{e['name']:e.get('value') for e in c.get('env',[]) if e['name'] in
                        ('WORKER_CORES','WORKER_MEMORY','SPARK_DAEMON_MEMORY')} for c in pod['spec']['containers']]})
    live.trace('worker_stability_precondition','FAIL' if unstable else 'PASS',unstable)
    live.save('worker_oom_evidence.json',unstable)
    assert not unstable,'ENVIRONMENT: repeated worker OOM within last 30 minutes; unsafe reliability baseline'


@case('RO-006','Latest actual scheduled batch completes rather than exhausting retries',6,kind='integration',services='Kubernetes,Spark,HDFS',timeout=60)
def test_live_scheduled_batch_completion(live):
    jobs=json.loads(live.kube('get','jobs','-n',live.live_namespace,'-o','json'))['items']
    jobs=sorted([j for j in jobs if j['metadata']['name'].startswith('batch-views-')],key=lambda j:j['metadata']['creationTimestamp'])
    if not jobs:pytest.skip('BLOCKED: no scheduled batch evidence')
    latest=jobs[-1];status=latest.get('status',{});name=latest['metadata']['name']
    live.trace('scheduled_batch','OBSERVED',{'name':name,'uid':latest['metadata']['uid'],'status':status})
    if status.get('active'):pytest.skip('INCONCLUSIVE: latest scheduled batch still running')
    if not status.get('succeeded'):
        pods=json.loads(live.kube('get','pods','-n',live.live_namespace,'-l','job-name='+name,'-o','json'))['items']
        logs=[]
        for pod in pods[:2]:
            logs.append({'pod':pod['metadata']['name'],'uid':pod['metadata']['uid'],'status':pod['status'],
                'log':live.kube('logs','-n',live.live_namespace,pod['metadata']['name'],'--tail=300')})
        live.save('scheduled_batch_failure.json',{'job':name,'status':status,'attempts':logs})
    assert status.get('succeeded')==1,'DEPLOYED PIPELINE: latest scheduled batch exhausted retries; see actual logs'
