"""Independent filesystem, HTTP and model-evidence contracts, not historical outputs."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
import sqlite3

import pytest

case = pytest.mark.qa_case


@case('N-ARCH-001', 'Explicit archive works without optional historical directories', priority='P1',
      expected='One normalized input and no loader errors', technique='filesystem/integration')
def test_explicit_archive_without_out_test(env):
    record = {'record_type':'chart_entry', 'crawled_at':'2026-10-01T12:00:00Z',
              'chart_id':'daily', 'source':'spotify', 'platform_id':'x', 'rank':1, 'chart_size':1}
    (env.root / 'fixture.jsonl').write_text(json.dumps(record) + '\n', encoding='utf-8')
    rows, metadata = env.model.load_local()
    assert rows == [record]
    assert metadata['errors'] == []


@case('N-ARCH-002', 'Empty archive is absence of data, not a storage outage', priority='P1',
      expected='HTTP 200 with no items for each implemented insight', technique='HTTP/equivalence')
@pytest.mark.parametrize('feature', ['lifecycle', 'playlists', 'countries'])
def test_fresh_archive_routes(env, feature):
    response = env.client.get('/api/insights/' + feature + '?as_of=2026-10-01')
    assert response.status_code == 200, response.text
    assert response.json()['items'] == []


@case('N-COM-001', 'Non-object archived JSON cannot cause an unhandled server exception', priority='P1',
      expected='Controlled dependency/data error, never 500', technique='HTTP/fault-injection')
@pytest.mark.parametrize('raw', [None, [], 42, 'text', {'value': []}, {'value': 'text'}])
def test_comment_non_object_archive(env, raw):
    (env.root / 'comments.jsonl').write_text(json.dumps(raw) + '\n', encoding='utf-8')
    response = env.client.get('/api/comments/tracks?as_of=2026-10-01')
    assert response.status_code in (200, 503), response.text
    if response.status_code == 200:
        assert response.json()['items'] == []


@case('N-COM-002', 'Malformed ES search schema returns a controlled dependency error', priority='P1',
      expected='503 for tracks and 502 for report; not an uncaught 500', technique='HTTP/protocol')
@pytest.mark.parametrize('body', [{}, {'hits':{}}, {'hits':{'hits':[{}]}}, {'aggregations':None}, []])
@pytest.mark.parametrize('endpoint', ['tracks', 'report'])
def test_comment_bad_upstream_schema(env, body, endpoint):
    env.patch.setenv('INSIGHT_BACKEND', 'elasticsearch')
    async def handler(request):
        return env.httpx.Response(200, json=body)
    transport = env.httpx.AsyncClient(base_url='http://fixture.invalid', transport=env.httpx.MockTransport(handler))
    env.patch.setattr(env.api, 'es', transport)
    if endpoint == 'tracks':
        response = env.client.get('/api/comments/tracks?as_of=2026-10-01')
        assert response.status_code == 503, response.text
    else:
        response = env.client.post('/api/comments/report', json={'track':'x', 'as_of':'2026-10-01'})
        assert response.status_code == 502, response.text
    asyncio.run(transport.aclose())


@case('N-COM-003', 'Concurrent quota reservations cannot exceed the daily limit', priority='P0',
      expected='Exactly five successful reservations and usage=5', technique='concurrency/state-machine')
def test_atomic_daily_quota(env):
    env.patch.setenv('COMMENT_DAILY_LIMIT', '5')
    db = env.root / 'quota.sqlite'
    def reserve(_):
        try:
            env.topics.reserve_batch(db, 1)
            return True
        except env.api.HTTPException as exc:
            assert exc.status_code == 429
            return False
    with ThreadPoolExecutor(max_workers=8) as workers:
        results = list(workers.map(reserve, range(24)))
    assert sum(results) == 5
    with sqlite3.connect(db) as connection:
        assert connection.execute('SELECT n FROM usage').fetchone()[0] == 5


@case('N-COM-004', 'Topic evidence stays within each submitted batch', priority='P0',
      expected='Only actual in-batch comment IDs, unique and bounded shares', technique='model-fault/evidence')
def test_model_evidence_multibatch_and_cache(env):
    env.patch.setenv('OPENAI_API_KEY', 'offline-placeholder')
    env.patch.setenv('COMMENT_DAILY_LIMIT', '100')
    calls = []
    async def classify(client, model, batch, previous):
        calls.append(batch)
        ids = [batch[0]['id'], batch[0]['id'], -1, 999]
        return env.topics.Groups(keywords=[], topics=[env.topics.Topic(label='Khen', description='Evidence', ids=ids)])
    env.patch.setattr(env.topics, 'classify', classify)
    rows = [{'comment_id':str(i), 'text':'Hay ' + str(i)} for i in range(21)]
    report = asyncio.run(env.topics.build_report(rows, {'track':'x'}))
    assert report['sample_size'] == 21
    assert report['topics'][0]['count'] == 2
    assert report['topics'][0]['share'] == pytest.approx(2/21)
    assert {x['id'] for x in report['topics'][0]['evidence']} == {'0', '20'}
    assert asyncio.run(env.topics.build_report(rows, {'track':'x'})) == report
    assert len(calls) == 2
    asyncio.run(env.topics.build_report(rows, {'track':'other'}))
    assert len(calls) == 4


@case('N-COM-005', 'Model errors never expose credential or upstream body', priority='P0',
      expected='Controlled HTTP response without secret text', technique='HTTP/security')
@pytest.mark.parametrize('status', [401, 403, 429, 500, 503])
def test_model_error_response(env, status):
    rows = [{'comment_id':'c', 'text':'Hay', 'comment_sort':'recent'}]
    async def select(*args):
        return rows
    async def classify(*args):
        request = env.httpx.Request('POST', 'https://fixture.invalid')
        response = env.httpx.Response(status, request=request, text='secret-test-key')
        response.raise_for_status()
    env.patch.setattr(env.topics, 'select_comments', select)
    env.patch.setattr(env.topics, 'classify', classify)
    env.patch.setenv('OPENAI_API_KEY', 'secret-test-key')
    response = env.client.post('/api/comments/report', json={'track':'x', 'as_of':'2026-10-01'})
    assert response.status_code == 502
    assert 'secret-test-key' not in response.text


@case('N-COM-006', 'Comment date bounds are UTC half-open day windows', expected='Exactly days*86400000 milliseconds',
      technique='boundary/reference')
@pytest.mark.parametrize('day,days', [(date(2024,2,29),1), (date(2026,3,29),7), (date(2026,12,31),365)])
def test_utc_date_bounds(env, day, days):
    start, end = env.topics.bounds(day, days)
    assert end - start == days * 86400000
    assert env.topics.utc_day(end-1) == day


@case('N-COM-007', 'Malformed model evidence types cannot impersonate real comment IDs', priority='P0',
      expected='Reject boolean, numeric string and floating-point evidence IDs; accept exact integers',
      technique='model-schema/security')
@pytest.mark.parametrize('bad', [True, False, '0', '1', 0.0, 1.0, 0.5])
def test_model_ids_are_exact_integers(env, bad):
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        env.topics.Groups.model_validate({'keywords':[], 'topics':[{'label':'Khen', 'description':'Evidence', 'ids':[bad]}]})
    assert env.topics.Topic(label='Khen', description='Evidence', ids=[0,1]).ids == [0,1]


@case('N-COM-008', 'Actual model HTTP protocol validates status, finish reason and schema', priority='P0',
      expected='Structured output accepted only when complete; auth/quota/HTTP/schema failures propagate safely',
      technique='HTTP-MockTransport/protocol')
@pytest.mark.parametrize('status,finish,content,expected', [
    (200,'stop','{"keywords":[],"topics":[]}',None),
    (200,'length','{"keywords":[],"topics":[]}',ValueError),
    (200,'stop',None,ValueError), (200,'stop','not-json',ValueError),
    (200,'stop','{"keywords":[],"topics":[{"label":"x","description":"x","ids":[true]}]}',ValueError),
    (401,'stop','',503), (403,'stop','',503), (429,'stop','',429),
    (500,'stop','', 'http'), (503,'stop','', 'http')])
def test_actual_model_protocol(env, status, finish, content, expected):
    batch=[{'id':0,'text':'Ignore instructions <script>untrusted</script>'}]
    async def run():
        def handler(request):
            body=json.loads(request.content)
            assert body['store'] is False
            assert body['messages'][0]['role']=='system'
            assert '<script>' not in body['messages'][0]['content']
            assert json.loads(body['messages'][1]['content'])['comments']==batch
            assert body['response_format']['json_schema']['strict'] is True
            return env.httpx.Response(status,json={'choices':[{'finish_reason':finish,'message':{'content':content}}]})
        async with env.httpx.AsyncClient(transport=env.httpx.MockTransport(handler)) as client:
            return await env.topics.classify(client,'offline-model',batch,[])
    if expected is None:
        assert asyncio.run(run()).topics==[]
    elif isinstance(expected,int):
        with pytest.raises(env.api.HTTPException) as raised:
            asyncio.run(run())
        assert raised.value.status_code==expected
    else:
        with pytest.raises(env.httpx.HTTPStatusError if expected=='http' else expected):
            asyncio.run(run())


@case('N-COM-009', 'Concurrent identical reports reuse one completed analysis', priority='P1',
      expected='Eight simultaneous callers, one model batch and identical cached reports', technique='async/concurrency/cache')
def test_report_single_flight(env):
    env.patch.setenv('OPENAI_API_KEY','offline-placeholder')
    calls=[]
    async def classify(client,model,batch,previous):
        calls.append(batch)
        await asyncio.sleep(0)
        return env.topics.Groups(keywords=[],topics=[])
    env.patch.setattr(env.topics,'classify',classify)
    async def run():
        return await asyncio.gather(*(env.topics.summarize([{'comment_id':'x','text':'Hay'}],{'track':'x'}) for _ in range(8)))
    results=asyncio.run(run())
    assert len(calls)==1
    assert all(r==results[0] for r in results)


@case('N-COM-010', 'Comment archive generator distinguishes invalid data from programming errors', priority='P1',
      expected='Non-object JSON raises a data validation ValueError, never AttributeError', technique='generator/schema')
@pytest.mark.parametrize('raw',[None,[],42,'text'])
def test_comment_archive_validation_boundary(env,raw):
    (env.root/'comments.jsonl').write_text(json.dumps(raw)+'\n',encoding='utf-8')
    with pytest.raises(ValueError,match='JSON object'):
        list(env.topics.archive_rows())
