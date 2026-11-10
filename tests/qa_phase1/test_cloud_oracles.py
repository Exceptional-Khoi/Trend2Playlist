"""Offline checks of observer contracts; no claim of distributed integration."""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'tests'))


@pytest.mark.qa_case('N-HARNESS-001','Cloud reconciliation follows the actual topic routing', priority='P0',
                     expected='All topics proven in HDFS; only chart/playlist/country topics in insight raw',
                     technique='protocol/observer-unit')
@pytest.mark.parametrize('topic', ['music.events','music.comments','music.charts','music.playlists','music.country-charts'])
def test_observer_topic_contract(tmp_path, monkeypatch, topic):
    from cloud import harness
    scope={'project':'qa-example','cluster':'music','location':'example',
           'context':'gke_qa-example_example_music', 'namespace':'music-e2e-qa20261110-test',
           'run_id':'qa20261110-test','approved_existing_nodes':True}
    observer=harness.Harness(scope,ROOT,tmp_path)
    observer.guard=lambda **kw:None
    observer.trace=lambda *a:None
    ack={'topic':topic,'partition':0,'offset':0,'doc_id':topic+':0:0','value':'{}'}
    raw=topic in ('music.charts','music.playlists','music.country-charts')
    def rpc(op,**kwargs):
        if op=='hdfs_records':return {'rows':[ack]}
        if op=='checkpoints':
            checkpoints={'ingest':{'offset_log':[{topic:{'0':1}}]}}
            if raw:checkpoints['insight-raw']={'offset_log':[{topic:{'0':1}}]}
            return checkpoints
        pytest.fail('Unexpected RPC '+op)
    observer.rpc=rpc
    def http(*args,**kwargs):
        assert raw, 'This topic is not routed into music-insight-raw by production'
        return {'status':200,'body':{'docs':[{'found':True,'_source':{'value':'{}'}}]}}
    observer.http=http
    def poll(fn,predicate,**kwargs):
        value=fn()
        assert predicate(value), 'Observer waits for a checkpoint the implementation does not produce'
        return value
    monkeypatch.setattr(harness,'poll',poll)
    assert observer.await_records([ack])['hdfs']['rows']==[ack]
