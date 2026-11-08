"""Real dependencies are opt-in and must be owned disposable LOCAL containers.

No URLs default to localhost:8000/9200 (those might forward a production cluster).
Missing service => explicit BLOCKED skip, never a mock-backed integration PASS.
"""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import uuid

import pytest

from .support import case, event


@pytest.fixture(scope='session')
def isolated_services():
    path=os.environ.get('QA_LOCAL_SERVICES_MANIFEST')
    if not path:pytest.skip('BLOCKED: no disposable local services manifest; running GCP excluded')
    manifest=json.loads(Path(path).read_text(encoding='utf-8'))
    if manifest.get('cleaned_up'):pytest.skip('BLOCKED: disposable local containers already cleaned up; provision a fresh owned environment')
    for container in manifest['containers']:
        # These immutable container IDs are owned for the whole test session.
        # Verify once, without dozens of redundant Windows Docker pipe calls.
        p=subprocess.run(['docker','inspect',container],capture_output=True,text=True,check=True,timeout=30)
        labels=json.loads(p.stdout)[0]['Config']['Labels']
        assert labels.get('trend2playlist.qa.campaign')==manifest['campaign'], 'Refusing unowned container'
    for key,url in manifest.items():
        if key.endswith('_url'):assert url.startswith('http://127.0.0.1:'), 'Refusing nonlocal service'
    return manifest


@case('R01','API reads real ES scroll pages and preserves stored events',technique='real ES/API integration',priority='P0',expected='HTTP 200 and one known event',environment='owned disposable local ES')
@pytest.mark.qa_integration
def test_real_es_api(api_env,isolated_services):
    import requests,httpx
    url=isolated_services.get('es_url')
    if not url:pytest.skip('BLOCKED: isolated ES absent')
    eid='qa-'+uuid.uuid4().hex;index='music-insight-raw'
    response=requests.put(url+'/'+index+'/_doc/'+eid+'?refresh=true',json={'value':json.dumps(event(eid))},timeout=15);response.raise_for_status()
    api_env.monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch')
    api_env.monkeypatch.setattr(api_env.api,'es',httpx.AsyncClient(base_url=url,timeout=15))
    try:
        r=api_env.client.get('/api/insights/feedback?as_of=2026-10-01')
        assert r.status_code==200 and any(x['group']=='x' and x['play']>=1 for x in r.json()['items'])
    finally:requests.delete(url+'/'+index+'/_doc/'+eid+'?refresh=true',timeout=15).raise_for_status()


@case('R02','Real ES partial bulk failure has per-item error evidence',technique='real ES/fault-injection',expected='Bulk errors true with one valid and one invalid mapping',environment='owned disposable local ES')
@pytest.mark.qa_integration
def test_real_es_partial_bulk(isolated_services):
    import requests
    url=isolated_services.get('es_url')
    if not url:pytest.skip('BLOCKED: isolated ES absent')
    index='qa-'+uuid.uuid4().hex
    requests.put(url+'/'+index,json={'mappings':{'properties':{'number':{'type':'integer'}}}},timeout=15).raise_for_status()
    try:
        body='\n'.join([json.dumps({'index':{'_index':index,'_id':'good'}}),json.dumps({'number':1}),
                        json.dumps({'index':{'_index':index,'_id':'bad'}}),json.dumps({'number':'not-an-integer'})])+'\n'
        r=requests.post(url+'/_bulk',data=body.encode(),headers={'Content-Type':'application/x-ndjson'},timeout=15);r.raise_for_status()
        assert r.json()['errors'] is True
        assert [x['index']['status'] for x in r.json()['items']]==[201,400]
    finally:requests.delete(url+'/'+index,timeout=15).raise_for_status()


@case('R03','Kafka real transport preserves IDs and explicitly exposes duplicate payloads',technique='real Kafka integration',expected='Producer acknowledgments and all 3 received records; no exactly-once assertion',environment='owned disposable local Kafka')
@pytest.mark.qa_integration
def test_real_kafka_duplicate_delivery(isolated_services):
    from kafka import KafkaProducer,KafkaConsumer
    from kafka.admin import KafkaAdminClient,NewTopic
    bootstrap=isolated_services.get('kafka_bootstrap')
    if not bootstrap:pytest.skip('BLOCKED: isolated Kafka absent')
    assert bootstrap.startswith('127.0.0.1:')
    topic='qa-'+uuid.uuid4().hex
    admin=KafkaAdminClient(bootstrap_servers=bootstrap,request_timeout_ms=10000)
    admin.create_topics([NewTopic(topic,1,1)])
    producer=KafkaProducer(bootstrap_servers=bootstrap,value_serializer=lambda x:json.dumps(x).encode())
    consumer=KafkaConsumer(topic,bootstrap_servers=bootstrap,auto_offset_reset='earliest',enable_auto_commit=False,
                           group_id='qa-'+uuid.uuid4().hex,consumer_timeout_ms=10000,value_deserializer=json.loads)
    try:
        for eid in ['one','one','two']:producer.send(topic,event(eid)).get(timeout=10)
        rows=list(consumer)
        assert [r.value['event_id'] for r in rows]==['one','one','two']
    finally:
        consumer.close();producer.close();admin.delete_topics([topic]);admin.close()


@case('R04','Spark file-source restart resumes checkpoint without rereading committed input',technique='real Spark/checkpoint integration',priority='P0',expected='Exactly the two input rows after stop/restart',environment='local PySpark + compatible Java; temporary files only')
@pytest.mark.qa_integration
def test_real_spark_checkpoint(tmp_path,monkeypatch,isolated_services):
    container=isolated_services.get('spark_container')
    if container:
        script=Path(__file__).with_name('spark_checkpoint_probe.py').read_text(encoding='utf-8')
        copied=subprocess.run(['docker','exec','-i',container,'python3','-c',
                              'import sys;open("/tmp/qa-checkpoint-probe.py","w").write(sys.stdin.read())'],
                              input=script,capture_output=True,text=True,timeout=30)
        assert copied.returncode==0,copied.stderr
        result=subprocess.run(['docker','exec',container,'/opt/spark/bin/spark-submit','--driver-memory','512m',
                               '/tmp/qa-checkpoint-probe.py'],capture_output=True,text=True,timeout=150)
        report=Path(os.environ['QA_REPORT_DIR'])/'spark-checkpoint.log'
        report.write_text(result.stdout+'\n'+result.stderr,encoding='utf-8')
        assert result.returncode==0,result.stderr[-4000:]
        assert 'QA_CHECKPOINT_RESULT=["a", "b"]' in result.stdout
        return
    if importlib.util.find_spec('pyspark') is None:pytest.skip('BLOCKED: PySpark unavailable')
    if os.environ.get('QA_ALLOW_LOCAL_SPARK')!='1':pytest.skip('BLOCKED: local Spark resource allocation not configured')
    from pyspark.sql import SparkSession
    monkeypatch.setenv('SPARK_LOCAL_IP','127.0.0.1')
    spark=SparkSession.builder.master('local[2]').appName('qa-checkpoint').config('spark.driver.host','127.0.0.1').config('spark.sql.shuffle.partitions','2').getOrCreate()
    incoming=tmp_path/'in';incoming.mkdir();out=tmp_path/'out';checkpoint=tmp_path/'checkpoint'
    query=None
    try:
        (incoming/'a.json').write_text('{"id":"a"}\n',encoding='utf-8')
        def start():return spark.readStream.schema('id string').json(str(incoming)).writeStream.format('parquet').option('checkpointLocation',str(checkpoint)).start(str(out))
        query=start();query.processAllAvailable();query.stop()
        (incoming/'b.json').write_text('{"id":"b"}\n',encoding='utf-8')
        query=start();query.processAllAvailable();query.stop()
        assert sorted(r.id for r in spark.read.parquet(str(out)).collect())==['a','b']
    finally:
        if query and query.isActive:query.stop()
        spark.stop()


@case('R05','Full-stack faults require a dedicated owned test cluster',technique='cluster fault/E2E plan',priority='P0',
      inputs='Broker outage, consumer restart, watermark, corrupt checkpoint, HDFS outage, state pressure',expected='NOT RUN until isolated full-stack harness exists; no production fault injection',environment='disposable Kafka+Spark+HDFS+ES+API cluster')
@pytest.mark.qa_integration
@pytest.mark.parametrize('scenario',['crawler_kafka_spark_hdfs_es_api','broker_outage','consumer_restart','late_watermark_boundary',
                                    'corrupted_checkpoint','hdfs_unavailable','schema_evolution','large_state_pressure'])
def test_full_stack_requires_isolation(scenario,isolated_services):
    pytest.skip('BLOCKED: full-stack '+scenario+' harness not provisioned; R01-R04 do not prove this scenario')


@case('R06','Production foreachBatch early return preserves real Spark state across microbatches',technique='real Spark/regression',priority='P0',
      expected='Second microbatch completes without missing state delta',environment='owned Spark container; actual callback; recommendation body stubbed to return early; not full application E2E')
@pytest.mark.qa_integration
def test_real_stateful_callback(isolated_services):
    container=isolated_services.get('spark_container')
    if not container:pytest.skip('BLOCKED: isolated Spark absent')
    script=Path(__file__).with_name('spark_state_probe.py').read_text(encoding='utf-8')
    copied=subprocess.run(['docker','exec','-i',container,'python3','-c',
                          'import sys;open("/tmp/qa-state-probe.py","w").write(sys.stdin.read())'],
                          input=script,capture_output=True,text=True,timeout=30)
    assert copied.returncode==0,copied.stderr
    result=subprocess.run(['docker','exec',container,'/opt/spark/bin/spark-submit','--driver-memory','512m',
                           '/tmp/qa-state-probe.py'],capture_output=True,text=True,timeout=150)
    (Path(os.environ['QA_REPORT_DIR'])/'spark-state.log').write_text(result.stdout+'\n'+result.stderr,encoding='utf-8')
    assert result.returncode==0,result.stderr[-5000:]
    assert 'QA_STATE_RESULT=two_batches_completed' in result.stdout


@case('R07','Spark master startup tolerates the service-link environment generated by its Service',technique='real Spark process/environment regression',priority='P0',
      expected='Master starts until bounded timeout, no NumberFormatException',environment='owned Spark container; Kubernetes-like env injection, not live Kubernetes')
@pytest.mark.qa_integration
def test_real_master_service_link(source_root,isolated_services):
    import yaml
    container=isolated_services.get('spark_container')
    if not container:pytest.skip('BLOCKED: isolated Spark absent')
    docs=list(yaml.safe_load_all((source_root/'k8s/40-spark-cluster.yaml').read_text(encoding='utf-8')))
    spec=next(d for d in docs if d and d.get('kind')=='Deployment' and d['metadata']['name']=='spark-master')['spec']['template']['spec']
    injected=[] if spec.get('enableServiceLinks') is False else ['-e','SPARK_MASTER_PORT=tcp://10.0.0.1:7077']
    explicit=next((e.get('value') for e in spec['containers'][0].get('env',[]) if e['name']=='SPARK_MASTER_PORT'),None)
    if explicit:injected=['-e','SPARK_MASTER_PORT='+explicit]
    result=subprocess.run(['docker','exec',*injected,container,'timeout','12s','/opt/spark/bin/spark-class',
        'org.apache.spark.deploy.master.Master','--host','127.0.0.1','--port','7077','--webui-port','8080'],
        capture_output=True,text=True,timeout=30)
    (Path(os.environ['QA_REPORT_DIR'])/'spark-master-env.log').write_text(result.stdout+'\n'+result.stderr,encoding='utf-8')
    assert result.returncode==124 and 'NumberFormatException' not in result.stderr,result.stderr[-4000:]
