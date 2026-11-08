"""Regression for cloud failures and blocked adversarial paths; real cloud proof is separate."""
import json
import os
from types import SimpleNamespace
import urllib.request

import pytest

from .support import event,isolated_function


def test_poisoned_raw_document_does_not_hide_valid_feedback(api_env):
    api_env.monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch')
    rows=[{'_id':'bad-offset','_source':{'value':'{"broken":'}},
          {'_id':'good-offset','_source':{'value':json.dumps(event('good'))}}]
    seen=[]
    def respond(request):
        seen.append(request.method)
        if request.method=='DELETE':return api_env.httpx.Response(200,json={})
        page=rows if len(seen)==1 else []
        return api_env.httpx.Response(200,json={'_scroll_id':'owned','hits':{'hits':page}})
    api_env.monkeypatch.setattr(api_env.api,'es',api_env.httpx.AsyncClient(transport=api_env.httpx.MockTransport(respond),base_url='http://es.test'))
    result=api_env.client.get('/api/insights/feedback?as_of=2026-10-01')
    assert result.status_code==200
    body=result.json()
    assert body['items'][0]['events']==1
    assert body['data']['partial'] is True and body['data']['invalid_records']==1
    assert body['data']['invalid_document_ids']==['bad-offset'] and 'DELETE' in seen


def test_only_invalid_addressable_raw_records_still_reject(api_env):
    api_env.monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch');pages=iter([
        [{'_id':'broken','_source':{'value':'not-json'}}],[]])
    def respond(request):
        return api_env.httpx.Response(200,json={} if request.method=='DELETE' else {'_scroll_id':'s','hits':{'hits':next(pages)}})
    api_env.monkeypatch.setattr(api_env.api,'es',api_env.httpx.AsyncClient(transport=api_env.httpx.MockTransport(respond),base_url='http://es.test'))
    assert api_env.client.get('/api/insights/feedback').status_code==422


def test_batch_checkpoint_is_run_scoped_and_cleanup_refuses_parent(source_root):
    calls=[];context=SimpleNamespace(setCheckpointDir=calls.append)
    module={'CKPT':'hdfs://owned/checkpoints'}
    create=isolated_function(source_root/'spark/jobs_common.py','batch_checkpoint_dir',module)
    first=create(SimpleNamespace(sparkContext=context),'run-a')
    second=create(SimpleNamespace(sparkContext=context),'run-b')
    assert first!=second and calls==[first,second]
    delete=isolated_function(source_root/'spark/jobs_common.py','cleanup_batch_checkpoint',
       {**module,'_fs':lambda *args:pytest.fail('Unsafe path reached filesystem')})
    for unsafe in ('hdfs://owned/checkpoints','hdfs://owned/checkpoints/batch-views/../other',
                   'hdfs://other/checkpoints/batch-views/run-a'):
        with pytest.raises(ValueError):delete(object(),unsafe)


def test_latest_bulk_uses_atomic_timestamp_guard_and_keeps_original_fields(source_root,monkeypatch):
    requests=[]
    class Response:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def read(self):return b'{"items":[{"update":{"status":200}}]}'
    def send(request,timeout):requests.append(request);return Response()
    monkeypatch.setattr(urllib.request,'urlopen',send)
    write=isolated_function(source_root/'spark/jobs_common.py','write_latest_partition',
       {'json':json,'urllib':SimpleNamespace(request=urllib.request),'time':SimpleNamespace(sleep=lambda _:None)})
    original={'doc_id':'same-slot','track_key':'old','crawled_ms':100}
    write(iter([original]),'http://owned:9200','music-rt-charts','doc_id','crawled_ms')
    header,payload=map(json.loads,requests[0].data.decode().splitlines())
    assert header['update']['_id']=='same-slot' and payload['script']['params']['doc']==original
    assert payload['scripted_upsert'] is True and "ctx.op = 'noop'" in payload['script']['source']
    assert 'params.doc[params.field] >= old' in payload['script']['source']


def test_latest_bulk_never_swallows_partial_rejection(source_root,monkeypatch):
    class Response:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def read(self):return b'{"items":[{"update":{"status":400}}]}'
    monkeypatch.setattr(urllib.request,'urlopen',lambda *a,**k:Response())
    write=isolated_function(source_root/'spark/jobs_common.py','write_latest_partition',
       {'json':json,'urllib':SimpleNamespace(request=urllib.request),'time':SimpleNamespace(sleep=lambda _:None)})
    with pytest.raises(RuntimeError,match='rejected document'):
        write(iter([{'doc_id':'x','crawled_ms':1}]),'http://owned:9200','idx','doc_id','crawled_ms')


@pytest.mark.parametrize('budget',['32','64','0','-1'])
def test_native_state_memory_is_bounded_and_invalid_budget_rejects(source_root,monkeypatch,budget):
    monkeypatch.setenv('STATE_STORE','rocksdb');monkeypatch.setenv('ROCKSDB_MEMORY_MB',budget)
    values={}
    class Builder:
        def appName(self,*a):return self
        def config(self,key,value):values[key]=value;return self
        def getOrCreate(self):return SimpleNamespace(sparkContext=SimpleNamespace(setLogLevel=lambda _:None))
    fn=isolated_function(source_root/'spark/jobs_common.py','get_spark',{'os':os,'SparkSession':SimpleNamespace(builder=Builder())})
    if int(budget)<=0:
        with pytest.raises(ValueError):fn('qa-native-budget')
    else:
        fn('qa-native-budget')
        assert values['spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage']=='true'
        assert values['spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB']==budget


@pytest.mark.parametrize('operation',['hdfs_records','publish','listen_parallel','crawl_apple'])
def test_observer_retries_reads_but_never_replays_writes(source_root,tmp_path,monkeypatch,operation):
    import io
    import urllib.error
    monkeypatch.syspath_prepend(str(source_root/'tests'))
    from cloud.harness import Harness
    scope={'project':'qa-example-project','cluster':'music','location':'example-zone',
           'context':'gke_qa-example-project_example-zone_music','namespace':'music-e2e-20261108-readretry',
           'run_id':'20261108-readretry','approved_existing_nodes':True}
    h=Harness(scope,source_root,tmp_path);h.guard=lambda *a,**k:None;h.agent_url='http://127.0.0.1:1'
    attempts=[]
    def request(*args,**kwargs):
        attempts.append(1)
        if len(attempts)==1:
            raise urllib.error.HTTPError('http://127.0.0.1:1',500,'observer timeout',{},
                 io.BytesIO(b'{"error":"TimeoutError","detail":"timed out"}'))
        return io.BytesIO(b'{"result":{"verified":true}}')
    monkeypatch.setattr(urllib.request,'urlopen',request)
    if operation=='hdfs_records':
        assert h.rpc(operation)=={'verified':True} and len(attempts)==2
    else:
        with pytest.raises(RuntimeError,match='TimeoutError'):h.rpc(operation)
        assert len(attempts)==1


@pytest.mark.parametrize('concurrent_creator',[False,True])
def test_empty_first_batch_publishes_empty_index_without_refresh_404(source_root,concurrent_creator):
    import io
    import urllib.error
    exists=[False];writes=[];calls=[]
    def request(method,path,body=None):
        calls.append((method,path))
        if path.endswith('/_search') or (path.endswith('/_refresh') and not exists[0]):
            raise urllib.error.HTTPError('http://owned'+path,404,'missing index',{},io.BytesIO(b'{}'))
        if method=='PUT':
            exists[0]=True
            if concurrent_creator:
                raise urllib.error.HTTPError('http://owned'+path,400,'index exists',{},
                   io.BytesIO(b'{"error":{"type":"resource_already_exists_exception"}}'))
        return {}
    fn=isolated_function(source_root/'spark/jobs_common.py','es_replace',
        {'ES_NODES':'owned','_es_request':request,'es_write':lambda *args:writes.append(args),'json':json})
    fn(object(),'empty-view','doc_id','run-a')
    assert exists[0] and len(writes)==1
    assert calls[-1]==('POST','/empty-view/_refresh')
    assert not any('_delete_by_query' in path for _,path in calls)


def test_webhdfs_observer_fails_over_and_revalidates_query_metadata(source_root,monkeypatch):
    import io
    import requests
    import threading
    import time
    import urllib.parse
    version=[1];content=[b'old-id'];calls=[]
    def stat(*args,**kwargs):return io.BytesIO(json.dumps({'FileStatus':{'modificationTime':version[0],'length':6}}).encode())
    monkeypatch.setattr(urllib.request,'urlopen',stat)
    dead='hdfs-datanode-0.hdfs-datanode.qa.svc.cluster.local:9864'
    good='hdfs-datanode-1.hdfs-datanode.qa.svc.cluster.local:9864'
    def get(url,**kwargs):
        calls.append(url)
        if 'hdfs-namenode:9870' in url:
            query=urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            host=good if dead in query.get('excludedatanodes',[''])[0] else dead
            return SimpleNamespace(status_code=307,headers={'Location':'http://'+host+'/webhdfs/v1/qa/run/checkpoints/metadata?op=OPEN'})
        if dead in url:raise requests.exceptions.ConnectTimeout('restarted DataNode')
        return SimpleNamespace(status_code=200,content=content[0])
    monkeypatch.setattr(requests,'get',get)
    fn=isolated_function(source_root/'tests/cloud/agent.py','hdfs',{'BASE':'/qa/run','NS':'qa',
        'HDFS_CACHE':{},'HDFS_CACHE_BYTES':0,'BAD_DATANODES':{},'HDFS_LOCK':threading.Lock(),
        'time':time,'io':io,'json':json,'urllib':SimpleNamespace(request=urllib.request,parse=urllib.parse)})
    path='/qa/run/checkpoints/metadata'
    assert fn(path,'OPEN')==b'old-id'
    assert any('excludedatanodes=' in url for url in calls)
    version[0]=2;content[0]=b'new-id'
    assert fn(path,'OPEN')==b'new-id','Cache must not hide changed checkpoint identity'


@pytest.mark.parametrize('condition',['healthy_hpa','no_hpa','not_ready','template_changed','restart'])
def test_workload_guard_distinguishes_autoscaling_from_degradation(source_root,tmp_path,monkeypatch,condition):
    import copy
    monkeypatch.syspath_prepend(str(source_root/'tests'))
    from cloud.harness import Harness
    from cloud.safety import SafetyError
    scope={'project':'qa-example-project','cluster':'music','location':'example-zone',
           'context':'gke_qa-example-project_example-zone_music','namespace':'music-e2e-20261108-hpaguard',
           'run_id':'20261108-hpaguard','approved_existing_nodes':True}
    h=Harness(scope,source_root,tmp_path)
    pod={'name':'api-old','uid':'pod1','app':'music-api','ready':True,'restarts':0,'deleting':False}
    original={'nodes':[{'name':'node1','allocatable':{'cpu':'4','memory':'8Gi'}}],
        'workloads':[pod],
        'controllers':{'Deployment/music-api':{'uid':'dep1','template_hash':'same','desired':4,'ready':4}},
        'hpas':{'Deployment/music-api':{'uid':'hpa1','spec_hash':'same','minimum':2}}}
    current=copy.deepcopy(original);current['workloads'][0]['name']='api-new'
    current['controllers']['Deployment/music-api'].update(desired=2,ready=2)
    if condition=='no_hpa':original['hpas']={};current['hpas']={}
    if condition=='not_ready':current['controllers']['Deployment/music-api']['ready']=1
    if condition=='template_changed':current['controllers']['Deployment/music-api']['template_hash']='changed'
    if condition=='restart':current['workloads'][0].update(name='api-old',restarts=1)
    h.baseline=original;h._baseline=lambda:current;h.request=lambda *a,**k:{'items':[]}
    if condition=='healthy_hpa':h.guard(force=True)
    else:
        with pytest.raises(SafetyError):h.guard(force=True)


@pytest.mark.parametrize('size',[3*1024*1024,5*1024*1024])
def test_readonly_client_accepts_valid_large_reply_but_keeps_finite_budget(source_root,monkeypatch,size):
    import io
    monkeypatch.syspath_prepend(str(source_root/'tests'))
    from cloud.readonly import ReadOnly
    from cloud.safety import SafetyError
    payload=json.dumps({'value':'a'*size}).encode()
    class Response(io.BytesIO):status=200
    monkeypatch.setattr(urllib.request,'urlopen',lambda *a,**k:Response(payload))
    h=object.__new__(ReadOnly);h.urls={'music-api':'http://127.0.0.1:1'};h.trace=lambda *a:None
    if size<4*1024*1024:
        result=h.read('/api/insights/lifecycle');assert result['status']==200 and len(result['body']['value'])==size
    else:
        with pytest.raises(SafetyError,match='budget exceeded'):h.read('/api/insights/lifecycle')
