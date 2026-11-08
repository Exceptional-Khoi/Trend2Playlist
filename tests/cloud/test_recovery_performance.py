import json
import math
import time
import urllib.parse

import pytest

from .cases import case
from .safety import poll
from .test_e2e_pipeline import feedback,hits


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
    assert feedback(dataset,dataset.track(),2)['play']==2


@pytest.mark.parametrize('app',['spark-worker','music-api','kafka','hdfs-datanode'])
@case('CR-002','Owned component restart retains acknowledged records',210,kind='reliability',destructive=True,timeout=500)
def test_component_restart(stack,dataset,app):
    before=dataset.publish([dataset.event('before')],'music.events');stack.await_records(before)
    inflight=dataset.publish([dataset.event('inflight')],'music.events')
    stack.restart(app)
    after=dataset.publish([dataset.event('after')],'music.events');stack.await_records(before+inflight+after,timeout=240)
    assert feedback(dataset,dataset.track(),3)['play']==3
    assert stack.http('/api/health')['status']==200


@case('CR-003','Real ES unavailable then restored with checkpoint retry recovery',220,kind='reliability',destructive=True,timeout=500)
def test_es_outage(stack,dataset):
    stack.await_records(dataset.publish([dataset.event('before')],'music.events'))
    try:
        stack.es_fault('unavailable')
        response=dataset.insights('feedback');stack.trace('outage_api','OBSERVED',response)
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
    assert feedback(dataset,dataset.track(),2)['play']==2


@case('CR-004','Real ES latency preserves accepted data and reports actual time',230,kind='reliability',destructive=True,timeout=300)
def test_dependency_latency(stack,dataset):
    try:
        stack.es_fault('latency')
        track,events,acks,result=dataset.listen(['play','skip'],workers=2)
        stack.trace('latency_fault_requests','MEASURED',result)
    finally:stack.es_fault('')
    stack.await_records(acks)
    assert feedback(dataset,track,2)['skip_rate']==.5


def percentiles(values):
    values=sorted(values)
    def p(q):
        x=(len(values)-1)*q;i=int(x);f=x-i
        return values[i]*(1-f)+values[min(i+1,len(values)-1)]*f
    return {'p50':p(.5),'p95':p(.95),'p99':p(.99),'samples':len(values)}


@case('CP-001','Bounded baseline moderate and peak HTTP load',800,kind='performance',priority='P1',timeout=500)
def test_controlled_load(stack,dataset):
    track=dataset.seed_catalog();stages=[];all_acks=[]
    for count,workers in [(4,1),(8,2),(16,4)]:
        stack.guard(force=True);start=time.monotonic()
        _,events,acks,result=dataset.listen(['play']*count,track=track,workers=workers)
        elapsed=time.monotonic()-start;all_acks+=acks
        latencies=[r['latency_ms'] for r in result['responses']]
        stages.append({'requests':count,'concurrency':workers,'wall_seconds':elapsed,
            'observed_throughput_rps':count/elapsed,'api_latency_ms':percentiles(latencies),
            'error_rate':sum(r['status']!=200 for r in result['responses'])/count})
    stack.await_records(all_acks,timeout=240)
    assert feedback(dataset,track,28)['play']==28
    ends=stack.rpc('end_offsets');checkpoints=stack.rpc('checkpoints')
    metrics=hits(stack,'music-metrics-history',{'match_all':{}})
    stack.trace('performance','MEASURED',{'stages':stages,'kafka_end_offsets':ends,'spark_checkpoint_offsets':checkpoints,
        'spark_metrics':metrics,'note':'No claimed SLA. Producer/API times include actual dependencies. Checkpoint lag is not Kafka group committed lag.'})
    stack.save('performance.json',{'stages':stages,'checkpoints':checkpoints,'end_offsets':ends,'spark_metrics':metrics})


@case('CP-002','Short sustained load and repeat memory observations',810,kind='performance',priority='P2',timeout=300)
def test_short_sustained_load(stack,dataset):
    track=dataset.seed_catalog();start=time.monotonic();acks=[];responses=[]
    # Six paced cohorts, bounded by both count and 45 seconds; no arbitrary readiness sleep.
    for i in range(6):
        if time.monotonic()-start>45:break
        stack.guard(force=True)
        _,_,positions,result=dataset.listen(['play','skip'],track=track,workers=2)
        acks+=positions;responses+=result['responses']
    elapsed=time.monotonic()-start;stack.await_records(acks)
    row=feedback(dataset,track,len(acks));assert row['skip_rate']==.5
    stack.trace('sustained_load','MEASURED',{'duration_seconds':elapsed,'events':len(acks),
        'throughput_rps':len(acks)/elapsed,'api_latency_ms':percentiles([r['latency_ms'] for r in responses]),
        'note':'Short bounded repeated cohorts; not a long-duration soak certification'})
