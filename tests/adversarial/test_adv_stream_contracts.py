import concurrent.futures
import json
import os
from pathlib import Path
import threading
from types import SimpleNamespace

import pytest

from .support import case, isolated_function


@case('H01','Early-returning recommendation callback releases persisted batch resources',technique='mocked callback/resource-contract',priority='P0',
      inputs='Nonempty DataFrame double and early-return body',expected='Callback invoked once with batch ID; unpersist called',risk='Persisted microbatch resource leak',environment='AST-executed production callback; actual state checkpoint checked separately in R06')
def test_stateful_batch_full_consumption(source_root):
    class Frame:
        consumed=0;released=False
        def persist(self):return self
        def isEmpty(self):self.consumed=1;return False
        def count(self):self.consumed=4;return 1
        def unpersist(self):self.released=True
    df=Frame()
    calls=[]
    callback=isolated_function(source_root/'spark/speed_layer.py','recommend_batch',{'_recommend':lambda *a:calls.append(a)})
    callback(df,1)
    assert df.released and calls==[(df,1)]


@case('H02','A failed micro-batch propagates failure and releases persisted data',technique='mocked callback/fault-injection',priority='P0',expected='Exception re-raised, unpersist called',environment='unit contract only')
def test_callback_failure_cleanup(source_root):
    class Frame:
        released=False
        def persist(self):return self
        def isEmpty(self):return False
        def count(self):return 1
        def unpersist(self):self.released=True
    frame=Frame()
    def fail(*a):raise RuntimeError('ES failure')
    fn=isolated_function(source_root/'spark/speed_layer.py','recommend_batch',{'_recommend':fail})
    with pytest.raises(RuntimeError):fn(frame,1)
    assert frame.released


@case('H03','ES connector writes use deterministic IDs and propagate save failure',technique='mocked connector/contract',expected='es.mapping.id retained; failed save not swallowed',environment='no real Elasticsearch/Spark')
@pytest.mark.parametrize('fail',[False,True])
def test_es_write_contract(source_root,fail):
    class Writer:
        options={}
        def format(self,name):self.format_name=name;return self
        def option(self,name,value):self.options={**self.options,name:value};return self
        def mode(self,value):return self
        def save(self):
            if fail:raise RuntimeError('partial bulk failure')
    writer=Writer();df=SimpleNamespace(write=writer)
    fn=isolated_function(source_root/'spark/jobs_common.py','es_write',{'ES_NODES':'isolated','ES_PORT':'9200'})
    if fail:
        with pytest.raises(RuntimeError):fn(df,'test-index','doc_id')
    else:fn(df,'test-index','doc_id')
    assert writer.options['es.mapping.id']=='doc_id'


@case('H04','Failed ES write must not delete previous batch',technique='mocked partial failure',priority='P0',expected='No cleanup delete if write failed',environment='unit contract only')
def test_es_replace_failed_write(source_root):
    calls=[]
    def fail(*a):raise RuntimeError('bulk partial failure')
    def request(*args):
        calls.append(args)
        return {'aggregations': {'runs': {'buckets': []}}}
    fn=isolated_function(source_root/'spark/jobs_common.py','es_replace',{'es_write':fail,'ES_NODES':'isolated','_es_request':request})
    with pytest.raises(RuntimeError):fn(object(),'idx','id','run1')
    # Replacement may perform a read-only preflight, but a failed write must
    # never refresh/delete or mutate previously published data.
    assert all(method=='POST' and path.endswith('/_search') for method,path,*_ in calls)


@case('H05','Concurrent batch publication cannot erase the newer successful run',technique='mocked concurrency/state-transition',priority='P0',
      inputs='A writes run1, B publishes run2, A resumes stale cleanup',expected='run2 remains available',risk='Delete-by-query race empties the index',environment='deterministic unit schedule, not ES integration')
def test_concurrent_es_replace(source_root):
    records={};a_written=threading.Event();b_done=threading.Event()
    def write(df,index,id_col):
        records['track']=df
        if df=='run1':
            a_written.set();assert b_done.wait(5)
    def request(method,path,body=None):
        if path.endswith('/_search'):
            return {'aggregations': {'runs': {'buckets': [{'key': value} for value in set(records.values())]}}}
        if '_delete_by_query' in path:
            previous=body['query']['terms']['run_id']
            for key in list(records):
                if records[key] in previous:del records[key]
    fn=isolated_function(source_root/'spark/jobs_common.py','es_replace',{'es_write':write,'ES_NODES':'isolated','_es_request':request})
    def second():
        assert a_written.wait(5)
        try:fn('run2','idx','id','run2')
        finally:b_done.set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        a=pool.submit(fn,'run1','idx','id','run1');b=pool.submit(second);a.result(timeout=10);b.result(timeout=10)
    assert records=={'track':'run2'}


@case('H06','Corrupt/unreadable batch-view pointers are not silently treated as absent',technique='mocked IO/fault-injection',expected='Permission/IO failure surfaced distinctly',risk='Serving silently uses missing/stale views',environment='mocked filesystem reader')
@pytest.mark.parametrize('error',[PermissionError('denied'),OSError('corrupt pointer')])
def test_view_corruption_visibility(source_root,error):
    class Reader:
        def json(self,path):raise error
    fn=isolated_function(source_root/'spark/jobs_common.py','read_view',{'VIEWS':'file:///isolated/views'})
    with pytest.raises(type(error)):fn(SimpleNamespace(read=Reader()),'catalog')


@case('J02','Local ES output IDs cannot write outside their index directory',technique='security/path-traversal',expected='No escaped file',environment='temporary filesystem only')
def test_local_es_put_path(source_root,tmp_path):
    fn=isolated_function(source_root/'spark/jobs_common.py','es_put',{'ES_NODES':'','LOCAL_ES_OUT':str(tmp_path/'out'),'os':os,'json':json})
    try:fn('safe','../../escaped',{'value':'qa'})
    except (ValueError,OSError):pass
    assert not (tmp_path/'escaped.json').exists()


@case('H07','Insight raw IDs include topic/partition/offset and have their own checkpoint',technique='static AST contract',expected='Stable Kafka-position ID and distinct insight checkpoint',environment='source inspection, no exactly-once claim')
def test_insight_stream_identity(source_root):
    text=(source_root/'spark/speed_layer.py').read_text(encoding='utf-8')
    assert "F.concat_ws(':', 'topic'" in text
    assert "F.col('partition')" in text and "F.col('offset')" in text
    assert "CKPT + '/insight-raw'" in text
