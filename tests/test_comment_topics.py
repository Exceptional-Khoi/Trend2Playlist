import json
import sys
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'serving'))
import api
import comment_topics as topics


def test_report_counts_evidence_and_reuses_sample(tmp_path, monkeypatch):
    monkeypatch.setenv('INSIGHT_BACKEND', 'archive')
    monkeypatch.setenv('INSIGHT_DATA_DIR', str(tmp_path))
    monkeypatch.setenv('OPENAI_API_KEY', 'test')
    monkeypatch.setenv('COMMENT_REPORT_DB', str(tmp_path/'reports.sqlite'))
    monkeypatch.setattr(topics, 'ROOT', tmp_path)
    start, end = topics.bounds(topics.date(2026,10,7), 7)
    base = dict(record_type='comment', track_key='song', title='Song', comment_sort='recent',
                published_ms=start, author_hash='private', text='Hay quá!')
    rows = [dict(base,comment_id='a'),dict(base,comment_id='a'),dict(base,comment_id='b'),
            dict(base,comment_id='outside',published_ms=end),dict(base,comment_id='blank',text=' ')]
    (tmp_path/'comments.jsonl').write_text('\n'.join(map(json.dumps,rows)),encoding='utf8')
    calls = []
    async def classify(client, model, batch, previous):
        calls.append(batch)
        assert all(set(row)=={'id','text'} for row in batch)
        return topics.Groups(keywords=[topics.Keyword(label='hay',variants=['Hay']),
                                       topics.Keyword(label='không có',variants=[])],
                             topics=[topics.Topic(label='Khen',description='Khán giả thích bài.',
                                                  ids=[0,1,1,100])])
    monkeypatch.setattr(topics,'classify',classify)
    client = TestClient(api.app)
    params = dict(as_of='2026-10-07', days=7, sort='recent')
    assert client.get('/api/comments/tracks',params=params).json()['items']==[{'key':'song','title':'Song'}]
    response = client.post('/api/comments/report',json=dict(params,track='song'))
    assert response.status_code==200
    report = response.json()
    assert report['sample_size']==report['keywords'][0]['count']==report['topics'][0]['count']==2
    assert len(report['keywords'])==1
    assert {entry['id'] for entry in report['topics'][0]['evidence']}=={'a','b'}
    assert client.post('/api/comments/report',json=dict(params,track='song')).json()==report
    assert len(calls)==1
    assert client.post('/api/comments/report',json=dict(params,track='missing')).status_code==404
    assert client.post('/api/comments/report',json=dict(params,track='song',limit=501)).status_code==422
