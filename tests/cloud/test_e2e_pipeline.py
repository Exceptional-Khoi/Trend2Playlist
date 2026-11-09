"""New service-level oracles. No imported application aggregation functions."""
import datetime as dt
import itertools
import json
import math
import time
import urllib.parse

import pytest

from .cases import case
from .safety import poll,sha256


def hits(stack,index,query):
    response=stack.http('/'+index+'/_search',method='POST',body={'size':200,'query':query},service='elasticsearch')
    if response['status']==404:return []
    assert response['status']==200,response
    return response['body']['hits']['hits']


def await_query_offsets(stack,name,acks):
    def covered(checkpoints):
        offsets={}
        for line in checkpoints.get(name,{}).get('offset_log',[]):
            if isinstance(line,dict):offsets.update({k:v for k,v in line.items() if k.startswith('music.')})
        return all(offsets.get(a['topic'],{}).get(str(a['partition']),-1)>a['offset'] for a in acks)
    result=poll(lambda:stack.rpc('checkpoints'),covered,timeout=150,interval=3,guard=stack.guard)
    stack.trace('typed_checkpoint_coverage','PASS',{'query':name,'checkpoints':result})
    return result


@case('CE-001','Versioned isolated deployment and real dependency readiness',0,kind='integration')
def test_deployed_identity_and_isolation(stack):
    assert not stack.list('Pod')==[]
    assert stack.get('ResourceQuota','qa-budget')['spec']['hard']['persistentvolumeclaims']=='0'
    assert all(s['spec'].get('type','ClusterIP')=='ClusterIP' for s in stack.list('Service'))
    for app,cm,path in [('music-api','serving-code','/app'),('speed-layer','spark-code','/opt/app')]:
        expected=stack.get('ConfigMap',cm)['data']
        script='import hashlib,json,pathlib; print(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in pathlib.Path('+repr(path)+').glob("*") if p.is_file()}))'
        actual=json.loads(stack.exec(app,['python3','-c',script]))
        assert all(actual.get(k)==sha256(v.encode()) for k,v in expected.items()),'Mounted application differs from audited source'
        stack.trace('mounted_source_hashes','PASS',{'app':app,'hashes':actual})
    assert stack.http('/api/health')['status']==200
    assert stack.http('/')['status']==200
    stack.trace('dependency_versions','OBSERVED',stack.rpc('versions'))
    stack.capture('identity')


@case('CE-002','Controlled HTTP source through actual Apple crawler to REST lifecycle',10)
def test_actual_crawler_to_api(stack,dataset):
    fixture={'feed':{'results':[{'id':dataset.track(),'name':'QA Đêm 🎵','artistName':'QA Artist',
        'genres':[{'name':'Pop'}],'url':'https://example.invalid/qa'}]}}
    result=stack.rpc('crawl_apple',{'fixture':fixture},timeout=120)
    stack.trace('crawler_produce','OBSERVED',{'fixture':fixture,**result})
    assert len(result['records'])==1
    record=result['records'][0];acks=result['acks']
    observed=stack.rpc('observe',{'positions':acks})
    assert [json.loads(x['value']) for x in observed]==result['records']
    stack.await_records(acks)
    body=dataset.ready('lifecycle',lambda b:any(x['track_id']==record['platform_id'] for x in b['items']),track=record['track_key'])
    row=next(x for x in body['items'] if x['track_id']==record['platform_id'])
    assert row['status']=='insufficient_data' and row['periods']==0
    assert row['title']=='QA Đêm 🎵'


@case('CE-003','Out-of-order source periods and replay preserve lifecycle trend',20)
def test_lifecycle_reordering_replay(stack,dataset):
    track=dataset.track();rows=[]
    for ago,rank in [(0,1),(2,3),(1,2)]:
        rows.append(dataset.chart(track,rank,3,dataset.day-dt.timedelta(days=ago)))
    acks=dataset.publish(rows+rows);stack.await_records(acks)
    body=dataset.ready('lifecycle',lambda b:len(b['items'])==1,track=track)
    assert body['items'][0]['status']=='rising'
    assert body['items'][0]['periods']==3
    assert [x['rank'] for x in body['items'][0]['history']]==[3,2,1]


@case('CE-004','Incomplete absence then full snapshot and returning track',30)
def test_lifecycle_partial_then_backfill(stack,dataset):
    track=dataset.track();other=dataset.track('other');day=dataset.day-dt.timedelta(days=1)
    stack.await_records(dataset.publish(dataset.chart_rows([track,other],day=day)))
    stack.await_records(dataset.publish([dataset.chart(other,1,2)]))
    body=dataset.ready('lifecycle',lambda b:bool(b['items']),track=track)
    assert body['items'][0]['status']!='left_chart','Incomplete snapshot cannot prove disappearance'
    # Newer complete capture supplies all positions, then a later period returns the original.
    stack.await_records(dataset.publish(dataset.chart_rows([other,dataset.track('replacement')],crawled_ms=dataset.now+1000)))
    body=dataset.ready('lifecycle',lambda b:b['items'] and b['items'][0]['status']=='left_chart',track=track)
    tomorrow=dataset.day+dt.timedelta(days=1)
    stack.await_records(dataset.publish(dataset.chart_rows([track,other],day=tomorrow,crawled_ms=dataset.now+2000)))
    body=dataset.ready('lifecycle',lambda b:b['items'] and b['items'][0]['periods']==2,track=track)
    assert body['items'][0]['status']!='left_chart'


@case('CE-005','Editorial placement score and 1/3/7 day late backfill',40)
def test_playlist_backfill_independent_score(stack,dataset):
    track=dataset.track();base=dataset.day-dt.timedelta(days=8)
    capture=int(dt.datetime.combine(base,dt.time(12),dt.timezone.utc).timestamp()*1000)
    chart=[dataset.chart(track,10,10,base),dataset.chart(track,7,10,base+dt.timedelta(days=1))]
    playlists=[{'record_type':'playlist_item','source':'spotify','platform_id':track,'track_key':track,
       'playlist_id':dataset.track('p'+str(i)),'playlist_type':kind,'position':pos,'playlist_size':10,
       'genre_hint':'Pop','crawled_ms':capture,'title':'QA','artists':['QA']}
       for i,(pos,kind) in enumerate([(1,'editorial'),(3,'editorial'),(1,'chart_mirror'),(1,'unknown')])]
    stack.await_records(dataset.publish(chart)+dataset.publish(playlists+playlists,'music.playlists'))
    body=dataset.ready('playlists',lambda b:any(x['track_id']==track for x in b['items']),genre='Pop')
    rows=[x for x in body['items'] if x['track_id']==track]
    assert {x['lag_days'] for x in rows}=={1}
    late=[dataset.chart(track,r,10,base+dt.timedelta(days=d)) for d,r in [(3,9),(7,10)]]
    stack.await_records(dataset.publish(late))
    body=dataset.ready('playlists',lambda b:len([x for x in b['items'] if x['track_id']==track])==3,genre='Pop')
    rows=[x for x in body['items'] if x['track_id']==track]
    assert {x['lag_days']:x['rank_improvement'] for x in rows}=={1:3,3:1,7:0}
    assert all(math.isclose(x['placement_score'],1/math.log2(2)+1/math.log2(4)) and x['playlist_count']==2 for x in rows)


@case('CE-006','Three-country Unicode set oracle and incomplete country backfill',50)
def test_country_independent_set_oracle(stack,dataset):
    a,b,c,d=[dataset.track(x) for x in ('đêm🎵','b','c','d')]
    countries={'VN':[a,b,c],'TH':[a,c,d],'US':[b,c,d]}
    rows=[r for country,tracks in countries.items() for r in dataset.chart_rows(tracks,country,chart_family=dataset.prefix)]
    missing=rows.pop();stack.await_records(dataset.publish(rows,'music.country-charts'))
    response=dataset.insights('countries',countries='VN,TH,US',chart_date=dataset.day.isoformat(),top_n=3)
    assert response['status']==200 and response['body']['overlap']==[]
    stack.await_records(dataset.publish([missing],'music.country-charts'))
    body=dataset.ready('countries',lambda b:len(b['overlap'])==3,countries='VN,TH,US',chart_date=dataset.day.isoformat(),top_n=3)
    for item in body['overlap']:
        x,y=item['countries'];sa,sb=set(countries[x]),set(countries[y])
        assert item['common']==len(sa&sb)
        assert math.isclose(item['jaccard'],len(sa&sb)/len(sa|sb))
    for item in body['items']:
        for country,tracks in countries.items():
            value=item['countries'][country]
            if item['track_id'] not in tracks:assert value is None
            else:
                rank=tracks.index(item['track_id'])+1
                assert value['rank']==rank and math.isclose(value['rank_score'],(4-rank)/3)
                assert math.isclose(value['chart_stream_share'],1/3)


@case('CE-009','Logical replay on distinct partitions agrees between realtime and insight',80)
def test_logical_duplicate_across_partitions(stack,dataset):
    event=dataset.event('replay');acks=[]
    for partition in (0,1):acks+=dataset.publish([event],'music.events',partition=partition)
    stack.await_records(acks)
    await_query_offsets(stack,'plays',acks)
    realtime=poll(lambda:hits(stack,'music-rt-plays',{'match':{'track_key':event['track_key']}}),timeout=120,interval=3,guard=stack.guard)
    stack.trace('realtime_documents','OBSERVED',realtime)
    assert sum(x['_source']['plays'] for x in realtime)==1,'Realtime double-counts the same logical event across Kafka offsets'


@case('CE-010','Late chart microbatch must not replace a newer published snapshot',90)
def test_late_chart_does_not_regress_latest(stack,dataset):
    new=dataset.chart(dataset.track('new'),crawled_ms=dataset.now)
    old=dataset.chart(dataset.track('old'),crawled_ms=dataset.now-86400000)
    stack.await_records(dataset.publish([new]))
    path='/api/charts/'+urllib.parse.quote(new['chart_id'],safe='')
    poll(lambda:stack.http(path),lambda r:r['status']==200 and any(x['track_key']==new['track_key'] for x in r['body']['items']),timeout=120,interval=3,guard=stack.guard)
    stack.await_records(dataset.publish([old]))
    # Raw checkpoint proves the older record arrived in a subsequent batch, typed sink observed below.
    docs=poll(lambda:hits(stack,'music-rt-charts',{'match':{'chart_id':new['chart_id']}}),timeout=90,interval=3,guard=stack.guard)
    stack.trace('latest_chart_documents','OBSERVED',docs)
    # Wait until the charts checkpoint has consumed the later Kafka offset, avoiding a false pass.
    # Observe a document with the late capture, or enough committed progress for a corrected implementation.
    end=stack.rpc('end_offsets')['music.charts']
    poll(lambda:stack.rpc('checkpoints'),lambda c:'charts' in c and any(isinstance(line,dict) and
      all(line.get('music.charts',{}).get(p,-1)>=o for p,o in end.items()) for line in c['charts']['offset_log']),timeout=120,interval=3,guard=stack.guard)
    result=stack.http(path);stack.trace('chart_api','OBSERVED',result)
    assert result['status']==200
    assert result['body']['items'][0]['track_key']==new['track_key'],'Latest chart regressed to an older capture'


@case('CE-011','Event-time minute boundaries differ from broker ingest day',100)
def test_event_time_windows(stack,dataset):
    minute=dataset.now//60000*60000
    records=[dataset.event('before',ts_ms=minute-1),dataset.event('at',ts_ms=minute)]
    acks=dataset.publish(records,'music.events');proof=stack.await_records(acks)
    docs=poll(lambda:hits(stack,'music-rt-plays',{'match':{'track_key':dataset.track()}}),lambda r:len(r)==2,timeout=120,interval=3,guard=stack.guard)
    assert {x['_source']['minute'] for x in docs}=={minute-60000,minute}
    assert sum(x['_source']['plays'] for x in docs)==2
    stack.trace('window_documents','PASS',docs)


@case('CE-012','Unknown nested schema fields preserved in raw but typed processing survives',110)
def test_additive_schema(stack,dataset):
    row=dataset.event('extra',future_schema={'unicode':'Đêm 🎵','nested':[None,{'x':1}]})
    acks=dataset.publish([row],'music.events');stack.await_records(acks)
    rt=poll(lambda:hits(stack,'music-rt-plays',{'match':{'track_key':row['track_key']}}),timeout=120,interval=3,guard=stack.guard)
    assert sum(r['_source']['plays'] for r in rt)==1


@case('CE-013','Real HDFS batch publishes catalog accessible through REST search',120,timeout=650)
def test_real_batch_catalog(stack,dataset):
    rows=dataset.chart_rows([dataset.track('one'),dataset.track('two')])
    stack.await_records(dataset.publish(rows))
    stack.batch()
    result=stack.http('/api/search?q='+urllib.parse.quote(dataset.prefix))
    stack.trace('batch_catalog_api','OBSERVED',result)
    assert result['status']==200 and len(result['body']['items'])>=2
    stack.batch_catalog_ready=True


@case('CE-014','Batch rerun on immutable history keeps equivalent logical catalog',130,timeout=650)
def test_batch_rerun_equivalence(stack):
    if not getattr(stack,'batch_catalog_ready',False):pytest.skip('BLOCKED: preceding real batch catalog failed')
    def logical():
        rows=hits(stack,'music-tracks',{'match_all':{}})
        return sorted([{k:v for k,v in x['_source'].items() if k not in ('run_id','computed_ms','updated_ms','run_ts_ms')} for x in rows],key=lambda x:json.dumps(x,sort_keys=True))
    before=logical();stack.batch();after=logical()
    stack.trace('batch_rerun','OBSERVED',{'before':before,'after':after})
    assert before==after,'Immutable raw history produced different logical batch catalog'


@case('CE-015','Malformed Kafka JSON cannot poison all independent insights',990,priority='P1')
def test_malformed_poison_pill_isolation(stack,dataset):
    valid=dataset.event('valid');bad=dataset.messages([valid],'music.events')[0];bad.pop('record');bad['raw']='{"broken":'
    acks=[]
    try:
        acks=stack.publish([bad]+dataset.messages([valid],'music.events'))
        stack.await_records(acks)
        response=dataset.insights('lifecycle');stack.trace('poison_api','OBSERVED',response)
        assert response['status']==200,'A single malformed Kafka message made chart analysis unavailable'
        assert response['body']['data']['partial'] is True
    finally:
        # Delete only the poisoned raw document in this owned, ephemeral ES instance.
        if acks:
            cleanup=stack.http('/music-insight-raw/_doc/'+urllib.parse.quote(acks[0]['doc_id'],safe='')+'?refresh=true',method='DELETE',service='elasticsearch')
            stack.trace('poison_cleanup','OBSERVED',cleanup)
