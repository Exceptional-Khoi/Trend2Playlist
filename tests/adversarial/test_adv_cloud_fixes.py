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


def test_full_profile_isolates_executor_processes_without_starving_batch(source_root):
    import re
    text=(source_root/'scripts/deploy.sh').read_text(encoding='utf-8')
    config=json.loads(re.search(r"k patch configmap music-env .*?-p '([^']+)'",text).group(1))['data']
    cores=int(re.search(r'k set env deployment/spark-worker .*WORKER_CORES=(\d+)',text).group(1))
    replicas=int(re.search(r'k scale deployment/spark-worker --replicas=(\d+)',text).group(1))
    assert cores==1, 'A three-GiB worker must not co-locate two Python-capable executors after the observed OOM'
    speed,batch=int(config['SPEED_CORES_MAX']),int(config['BATCH_CORES_MAX'])
    assert speed>0 and batch>0 and speed+batch<=cores*replicas, 'Long-lived speed executors must leave a batch slot'


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
    dead='hdfs-datanode-0.hdfs-datanode.qa.svc.cluster.local:9864'
    good='hdfs-datanode-1.hdfs-datanode.qa.svc.cluster.local:9864'
    def stat(url,**kwargs):
        value={'FileStatus':{'modificationTime':version[0],'length':6}}
        if 'GET_BLOCK_LOCATIONS' in url:
            value={'LocatedBlocks':{'locatedBlocks':[{'locations':[
                {'hostName':dead.split(':')[0],'ipAddr':'10.0.0.10','xferPort':9866},
                {'hostName':good.split(':')[0],'ipAddr':'10.0.0.11','xferPort':9866}]}]}}
        return io.BytesIO(json.dumps(value).encode())
    monkeypatch.setattr(urllib.request,'urlopen',stat)
    def get(url,**kwargs):
        calls.append(url)
        if 'hdfs-namenode:9870' in url:
            query=urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            host=good if '10.0.0.10:9866' in query.get('excludedatanodes',['']) else dead
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
    exclusions=[urllib.parse.parse_qs(urllib.parse.urlparse(url).query)['excludedatanodes'][0]
        for url in calls if 'excludedatanodes=' in url]
    assert exclusions==['10.0.0.10:9866'],'NameNode matches the registered transfer address, not redirect DNS/HTTP port'
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


def test_observer_reads_only_committed_parquet_not_orphan_directory(source_root):
    import urllib.parse
    base='/qa/run';root=base+'/datalake/raw';path=root+'/topic=music.events/dt=2026-11-08/a.parquet'
    calls=[]
    def hdfs(path_,op='LISTSTATUS'):
        calls.append((path_,op))
        if path_==root+'/_spark_metadata':return {'FileStatuses':{'FileStatus':[{'pathSuffix':'0'}]}}
        assert path_==root+'/_spark_metadata/0', 'Observer must not scan uncommitted physical files'
        return ('v1\n'+json.dumps({'path':'hdfs://hdfs-namenode:8020'+path,'size':12,'isDir':False,'action':'add'})).encode()
    fn=isolated_function(source_root/'tests/cloud/agent.py','committed_parquet_files',
        {'BASE':base,'hdfs':hdfs,'json':json,'urllib':SimpleNamespace(parse=urllib.parse)})
    result=fn()
    assert [f['path'] for f in result['files']]==[path] and result['logs']==['0']
    assert len(calls)==2


def test_observer_latest_compact_log_applies_deletes_and_uri_decoding(source_root):
    import urllib.parse
    base='/qa/run';root=base+'/datalake/raw'
    def row(name,action='add'):
        path=root+'/topic=music.events/dt=2026-11-08/'+name+'.parquet'
        return {'path':'hdfs://hdfs-namenode:8020'+urllib.parse.quote(path),'size':12,'isDir':False,'action':action}
    logs={'9.compact':[row('a'),row('b')],'10':[row('a','delete'),row('đêm🎵')]}
    def hdfs(path,op='LISTSTATUS'):
        if op=='LISTSTATUS':return {'FileStatuses':{'FileStatus':[{'pathSuffix':n} for n in ['10','0','9.compact','.temporary']]}}
        name=path.rsplit('/',1)[-1];assert name!='0', 'Old logs must not resurrect deleted files after compact snapshot'
        return ('v1\n'+'\n'.join(json.dumps(r) for r in logs[name])).encode()
    fn=isolated_function(source_root/'tests/cloud/agent.py','committed_parquet_files',
        {'BASE':base,'hdfs':hdfs,'json':json,'urllib':SimpleNamespace(parse=urllib.parse)})
    result=fn()
    assert result['logs']==['9.compact','10']
    assert [r['path'].rsplit('/',1)[-1] for r in result['files']]==['b.parquet','đêm🎵.parquet']


def test_hdfs_observer_preserves_committed_offsets_ignoring_empty_orphan(source_root,monkeypatch):
    import io
    import sys
    import types
    import urllib.parse
    base='/qa/run';root=base+'/datalake/raw';partition=root+'/topic=music.events/dt=2026-11-08'
    valid=partition+'/b.parquet';orphan=partition+'/a.parquet';payload=b'PAR1xxxxPAR1';opened=[]
    row={'topic':'music.events','partition':0,'offset':7,'value':'{}'}
    parent=types.ModuleType('pyarrow');pq=types.ModuleType('pyarrow.parquet');parent.parquet=pq
    def read_table(data):
        if data.getvalue()!=payload:raise ValueError('Uncommitted empty Parquet reached codec')
        return SimpleNamespace(to_pylist=lambda:[row])
    pq.read_table=read_table
    monkeypatch.setitem(sys.modules,'pyarrow',parent);monkeypatch.setitem(sys.modules,'pyarrow.parquet',pq)
    def directory(names):return {'FileStatuses':{'FileStatus':names}}
    def hdfs(path,op='LISTSTATUS'):
        if op=='OPEN':
            opened.append(path)
            if path==root+'/_spark_metadata/0':
                return ('v1\n'+json.dumps({'path':'hdfs://nn:8020'+valid,'size':len(payload),'isDir':False,'action':'add'})).encode()
            return payload if path==valid else b''
        if path==root+'/_spark_metadata':return directory([{'pathSuffix':'0'}])
        if path==root:return directory([{'pathSuffix':'topic=music.events','type':'DIRECTORY'}])
        if path==root+'/topic=music.events':return directory([{'pathSuffix':'dt=2026-11-08','type':'DIRECTORY'}])
        assert path==partition
        return directory([{'pathSuffix':'a.parquet','type':'FILE','length':0},
                          {'pathSuffix':'b.parquet','type':'FILE','length':len(payload)}])
    namespace={'BASE':base,'hdfs':hdfs,'json':json,'urllib':SimpleNamespace(parse=urllib.parse)}
    committed=isolated_function(source_root/'tests/cloud/agent.py','committed_parquet_files',namespace)
    fn=isolated_function(source_root/'tests/cloud/agent.py','hdfs_records',
        dict(namespace,committed_parquet_files=committed,PARQUET_CACHE={},io=io))
    result=fn()
    assert [(r['partition'],r['offset']) for r in result['rows']]==[(0,7)]
    assert result['files']==1 and orphan not in opened and valid in opened


@pytest.mark.parametrize('path',['hdfs://nn/qa/other/file.parquet','hdfs://nn/qa/run/datalake/raw/../file.parquet'])
def test_observer_rejects_committed_paths_outside_own_dataset(source_root,path):
    import urllib.parse
    def hdfs(p,op='LISTSTATUS'):
        if op=='LISTSTATUS':return {'FileStatuses':{'FileStatus':[{'pathSuffix':'0'}]}}
        return ('v1\n'+json.dumps({'path':path,'size':12,'isDir':False,'action':'add'})).encode()
    fn=isolated_function(source_root/'tests/cloud/agent.py','committed_parquet_files',
        {'BASE':'/qa/run','hdfs':hdfs,'json':json,'urllib':SimpleNamespace(parse=urllib.parse)})
    with pytest.raises(ValueError,match='outside owned'):fn()


@pytest.mark.parametrize('payload',[b'',b'PAR1bad',b'badbytes'])
def test_committed_empty_truncated_or_corrupt_parquet_never_silently_skipped(source_root,monkeypatch,payload):
    import io
    import sys
    import types
    parent=types.ModuleType('pyarrow');pq=types.ModuleType('pyarrow.parquet');parent.parquet=pq
    def corrupt(data):raise ValueError('Invalid Parquet footer')
    pq.read_table=corrupt
    monkeypatch.setitem(sys.modules,'pyarrow',parent);monkeypatch.setitem(sys.modules,'pyarrow.parquet',pq)
    path='/qa/run/datalake/raw/topic=music.events/dt=2026-11-08/bad.parquet'
    fn=isolated_function(source_root/'tests/cloud/agent.py','hdfs_records',
        {'committed_parquet_files':lambda:{'files':[{'path':path,'size':8}],'logs':['0']},
         'hdfs':lambda *a:payload,'PARQUET_CACHE':{},'io':io})
    with pytest.raises(ValueError,match='Committed Parquet payload invalid|Invalid Parquet footer'):fn()


def test_restart_does_not_accept_other_preexisting_ready_replica(source_root,tmp_path,monkeypatch):
    import copy
    monkeypatch.syspath_prepend(str(source_root/'tests'))
    from cloud import harness
    from cloud.stack import LABEL
    scope={'project':'qa-example-project','cluster':'music','location':'example-zone',
        'context':'gke_qa-example-project_example-zone_music','namespace':'music-e2e-20261108-restart',
        'run_id':'20261108-restart','approved_existing_nodes':True}
    h=harness.Harness(scope,source_root,tmp_path)
    def pod(uid,name,ready=True):return {'metadata':{'uid':uid,'name':name,'namespace':h.namespace,
        'labels':{'app':'hdfs-datanode',LABEL:h.run_id}},'status':{'containerStatuses':[{'ready':ready}]}}
    old=pod('old','dn-0');survivor=pod('survivor','dn-1');new=pod('new','dn-0')
    booting=copy.deepcopy(new);booting['status']['containerStatuses'][0]['ready']=False
    snapshots=iter([[old,survivor],[survivor],[survivor,booting],[survivor,new]])
    h.list=lambda kind:next(snapshots);h.pod=lambda app:old;h.guard=lambda **kw:None
    deleted=[];traces=[];h.delete=lambda kind,name:deleted.append((kind,name));h.trace=lambda *a:traces.append(a)
    def poll(fn,predicate,**kw):
        assert fn() is None, 'Surviving replica is not the replacement'
        assert fn() is None, 'A newly created unready pod is not recovered'
        result=fn();assert predicate(result);return result
    monkeypatch.setattr(harness,'poll',poll)
    h.restart('hdfs-datanode')
    assert deleted==[('Pod','dn-0')]
    assert traces[-1][2]['new_uid']=='new'


@pytest.mark.parametrize('transient',[True,False])
def test_hdfs_eventual_poll_retries_transport_but_propagates_committed_corruption(source_root,tmp_path,monkeypatch,transient):
    monkeypatch.syspath_prepend(str(source_root/'tests'))
    from cloud import harness
    scope={'project':'qa-example-project','cluster':'music','location':'example-zone',
        'context':'gke_qa-example-project_example-zone_music','namespace':'music-e2e-20261108-observation',
        'run_id':'20261108-observation','approved_existing_nodes':True}
    h=harness.Harness(scope,source_root,tmp_path);h.guard=lambda **kw:None;traces=[];h.trace=lambda *a:traces.append(a)
    ack={'topic':'music.events','partition':0,'offset':0,'value':'{}','doc_id':'music.events:0:0'}
    reads=[]
    def rpc(op,**kw):
        if op=='checkpoints':return {name:{'offset_log':[{'music.events':{'0':1}}]} for name in ['ingest','insight-raw']}
        assert op=='hdfs_records';reads.append(kw)
        if len(reads)==1:
            error=harness.ObservationUnavailable if transient else RuntimeError
            raise error('temporary transport' if transient else 'Committed Parquet payload invalid')
        return {'rows':[ack]}
    h.rpc=rpc
    h.http=lambda *a,**kw:{'status':200,'body':{'docs':[{'found':True,'_source':{'value':'{}'}}]}}
    def poll(fn,predicate,**kw):
        for _ in range(2):
            value=fn()
            if predicate(value):return value
        raise AssertionError('Fixture did not recover within deterministic polling steps')
    monkeypatch.setattr(harness,'poll',poll)
    if transient:
        result=h.await_records([ack],timeout=150)
        assert result['hdfs']['rows']==[ack] and len(reads)==2
        assert all(r['timeout']<=30 and r['read_retries']==1 for r in reads)
        assert any(t[0]=='observer_eventual_retry' for t in traces)
    else:
        with pytest.raises(RuntimeError,match='Committed Parquet'):h.await_records([ack])
        assert len(reads)==1 and not traces


@pytest.mark.parametrize('selection_exhausted',[True,False])
def test_webhdfs_selection_403_is_transient_but_authorization_403_remains_failure(source_root,monkeypatch,selection_exhausted):
    import io
    import requests
    import threading
    import time
    import urllib.error
    import urllib.parse
    bad={'10.0.0.10:9866':time.monotonic()+20}
    remote={'exception':'IOException','message':'No active nodes contain this block'} if selection_exhausted else {
        'exception':'AccessControlException','message':'Permission denied'}
    monkeypatch.setattr(urllib.request,'urlopen',lambda *a,**kw:io.BytesIO(
        b'{"FileStatus":{"modificationTime":1,"length":10}}'))
    monkeypatch.setattr(requests,'get',lambda *a,**kw:SimpleNamespace(status_code=403,
        content=json.dumps({'RemoteException':remote}).encode()))
    fn=isolated_function(source_root/'tests/cloud/agent.py','hdfs',{'BASE':'/qa/run','NS':'qa',
        'HDFS_CACHE':{},'HDFS_CACHE_BYTES':0,'BAD_DATANODES':bad,'HDFS_LOCK':threading.Lock(),
        'time':time,'io':io,'json':json,'urllib':SimpleNamespace(request=urllib.request,error=urllib.error,parse=urllib.parse)})
    if selection_exhausted:
        with pytest.raises(TimeoutError,match='exclusions exhausted'):fn('/qa/run/checkpoints/id','OPEN')
        assert not bad
    else:
        with pytest.raises(urllib.error.HTTPError) as error:fn('/qa/run/checkpoints/id','OPEN')
        assert error.value.code==403 and bad
