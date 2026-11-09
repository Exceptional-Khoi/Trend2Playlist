import json
import math
import time
import urllib.parse

import pytest

from .cases import case
from .safety import poll
from .test_e2e_pipeline import hits
from .storage_audit import FRESH_HDFS_CODE,assert_fresh_acks


@case('CR-001','Driver restart resumes same checkpoint query without raw duplication',200,kind='reliability',destructive=True,timeout=500)
def test_driver_checkpoint_recovery(stack,dataset):
    first=dataset.publish([dataset.event('before')],'music.events');stack.await_records(first)
    before=stack.rpc('checkpoints');stack.restart('speed-layer')
    poll(lambda:stack.rpc('checkpoints'),lambda c:'plays' in c,timeout=120,interval=3,guard=stack.guard)
    second=dataset.publish([dataset.event('after')],'music.events');proof=stack.await_records(second,timeout=240)
    after=stack.rpc('checkpoints')
    assert all(after[k]['query_id']==before[k]['query_id'] for k in before if k in after)
    observed=stack.rpc('hdfs_records')['rows']
    for ack in first+second:
        assert sum((r['topic'],r['partition'],r['offset'])==(ack['topic'],ack['partition'],ack['offset']) for r in observed)==1
    poll(lambda: hits(stack,'music-rt-plays',{'match':{'track_key':dataset.track()}}),
         lambda rows: sum(r['_source']['plays'] for r in rows)==2,timeout=120,interval=3,guard=stack.guard)


@pytest.mark.parametrize('app',['spark-worker','music-api','kafka','hdfs-datanode'])
@case('CR-002','Owned component restart retains acknowledged records',210,kind='reliability',destructive=True,timeout=500)
def test_component_restart(stack,dataset,app):
    before=dataset.publish([dataset.event('before')],'music.events');stack.await_records(before)
    inflight=dataset.publish([dataset.event('inflight')],'music.events')
    stack.restart(app)
    after=dataset.publish([dataset.event('after')],'music.events');stack.await_records(before+inflight+after,timeout=240)
    poll(lambda: hits(stack,'music-rt-plays',{'match':{'track_key':dataset.track()}}),
         lambda rows: sum(r['_source']['plays'] for r in rows)==3,timeout=120,interval=3,guard=stack.guard)
    assert stack.http('/api/health')['status']==200
    if app=='hdfs-datanode':
        fresh=json.loads(stack.exec('qa-agent',['python','-u','-c',FRESH_HDFS_CODE],timeout=120))
        assert_fresh_acks(before+inflight+after,fresh)
        stack.trace('fresh_storage_after_fault','PASS',fresh)


@case('CR-003','Real ES unavailable then restored with checkpoint retry recovery',220,kind='reliability',destructive=True,timeout=500)
def test_es_outage(stack,dataset):
    stack.await_records(dataset.publish([dataset.event('before')],'music.events'))
    try:
        stack.es_fault('unavailable')
        response=dataset.insights('lifecycle');stack.trace('outage_api','OBSERVED',response)
        assert response['status']==503,'Dependency failure must not silently fall back to archive'
        pending=dataset.publish([dataset.event('during')],'music.events')
        published_ms=int(time.time()*1000)
        def observed_sink_attempt():
            text=stack.kube('logs','-n',stack.namespace,'deployment/es-gate','--tail=150')
            values=[]
            for line in text.splitlines():
                if line.startswith('QA_ES_FAULT '):values.append(json.loads(line[len('QA_ES_FAULT '):]))
            # No API reads here; health probes only use /_cluster/health. A new non-health
            # connector request proves the actual Spark sink encountered the outage.
            return [v for v in values if v['time_ms']>=published_ms and v['path'].split('?')[0]!='/_cluster/health']
        attempts=poll(observed_sink_attempt,timeout=70,interval=3,guard=stack.guard)
        stack.trace('real_sink_outage_observed','PASS',attempts)
    finally:stack.es_fault('')
    stack.await_records(pending,timeout=240)
    poll(lambda: hits(stack,'music-rt-plays',{'match':{'track_key':dataset.track()}}),
         lambda rows: sum(r['_source']['plays'] for r in rows)==2,timeout=120,interval=3,guard=stack.guard)


@case('CR-004','Real ES latency still permits chart reads',230,kind='reliability',destructive=True,timeout=300)
def test_dependency_latency(stack,dataset):
    try:
        stack.es_fault('latency')
        result=dataset.insights('lifecycle')
        stack.trace('latency_fault_requests','MEASURED',result)
        assert result['status']==200
    finally:
        stack.es_fault('')
