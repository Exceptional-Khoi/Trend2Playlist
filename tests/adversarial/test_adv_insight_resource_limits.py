"""Regression for observed cloud OOM: overlapping history reads and retained JSON copies."""
import asyncio
import itertools
import json
import tracemalloc

import pytest


@pytest.mark.parametrize('second_feature,expected_reads',[('countries',1),('lifecycle',2)])
def test_heavy_http_queries_are_bounded_but_health_stays_available(api_env,second_feature,expected_reads):
    env=api_env
    env.monkeypatch.setenv('INSIGHT_BACKEND','elasticsearch')
    async def run():
        env.monkeypatch.setattr(env.api,'_insight_work_lock',asyncio.Lock(),raising=False)
        entered=asyncio.Event();release=asyncio.Event();reads=0
        async def handler(request):
            nonlocal reads
            if request.url.path=='/music-insight-raw/_search':
                reads+=1;entered.set();await release.wait()
                return env.httpx.Response(200,json={'hits':{'hits':[]}})
            if request.url.path=='/_cluster/health':
                return env.httpx.Response(200,json={'status':'green'})
            raise AssertionError('Unexpected dependency path '+request.url.path)
        async with env.httpx.AsyncClient(base_url='http://es.invalid',transport=env.httpx.MockTransport(handler)) as dependency:
            env.monkeypatch.setattr(env.api,'es',dependency)
            async with env.httpx.AsyncClient(base_url='http://app',transport=env.httpx.ASGITransport(app=env.api.app)) as client:
                first=asyncio.create_task(client.get('/api/insights/countries?days=7'))
                await asyncio.wait_for(entered.wait(),2)
                second=asyncio.create_task(client.get('/api/insights/'+second_feature+'?days=7'))
                # FIFO event-loop barrier: second gets a turn; no timing sleep.
                scheduled=asyncio.Event();asyncio.get_running_loop().call_soon(scheduled.set)
                await scheduled.wait()
                try:
                    assert reads==1,'Two raw-history working sets were loaded concurrently'
                    health=await asyncio.wait_for(client.get('/api/health'),2)
                    assert health.status_code==200
                finally:release.set()
                responses=await asyncio.wait_for(asyncio.gather(first,second),4)
                assert [r.status_code for r in responses]==[200,200]
                assert reads==expected_reads,'Duplicate in-flight request should use the completed response'
    asyncio.run(run())


def test_snapshot_selection_keeps_tie_order_and_whole_capture(model):
    base=dict(record_type='chart_entry',source='zing',chart_id='c',country_code='VN',period_type='daily',
              source_period='2026-11-10',ts_ms=1794268800000,rank=1,chart_size=1,platform_id='a',title='A')
    rows=[base,dict(base,title='Z'),dict(base,ts_ms=1794268860000,title='Latest')]
    for order in itertools.permutations(rows):
        snapshots=model.chart_snapshots(list(order)+[dict(base)])
        assert len(snapshots)==1 and snapshots[0][0]['title']=='Latest'
    for order in itertools.permutations(rows[:2]):
        assert model.chart_snapshots(list(order))[0][0]['title']=='Z'


def test_snapshot_working_memory_does_not_retain_multiple_full_json_copies(model):
    rows=[dict(record_type='chart_entry',source='zing',chart_id='chart-'+str(i//40),country_code='VN',
        period_type='daily',source_period='2026-11-10',ts_ms=1794268800000+(i//40)*1000,
        platform_id='t'+str(i%40),title='Song',rank=i%40+1,chart_size=40,
        provider_metadata='x'*4096+str(i)) for i in range(4000)]
    serialized_bytes=sum(len(json.dumps(r,sort_keys=True,ensure_ascii=False).encode('utf8')) for r in rows)
    tracemalloc.start()
    try:
        snapshots=model.chart_snapshots(rows)
        _,peak=tracemalloc.get_traced_memory()
    finally:tracemalloc.stop()
    assert len(snapshots)==100 and all(model.complete(s) for s in snapshots)
    # Budget: one transient canonical payload plus 60% bookkeeping headroom.
    # This is a regression bound for this fixture, not a production SLA/RSS claim.
    assert peak<serialized_bytes*1.6,{'peak_python_bytes':peak,'canonical_input_bytes':serialized_bytes}
