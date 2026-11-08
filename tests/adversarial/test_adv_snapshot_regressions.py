"""Real crawler snapshot buckets must not inflate observed source periods."""
from .support import case,chart,full_chart


@case('FX19','Multiple crawls in one rounded snapshot bucket remain one source period',technique='state-transition/metamorphic',priority='P0',
      inputs='Three recrawls at 12:00/12:10/12:20 with same snapshot_id/source_period',expected='Latest whole crawl only; periods1 and insufficient_data')
def test_recrawls_in_same_snapshot_bucket(model):
    rows=[]
    for minute,tracks in [(0,['x','y']),(10,['y','x']),(20,['x','y'])]:
        rows+=full_chart(tracks,snapshot_id='rounded-30m',crawled_at=f'2026-10-01T12:{minute:02}:00Z')
    for data in (rows,rows[::-1]):
        items=model.analyze_track_lifecycle(data)['items']
        assert len(items)==2
        assert all(r['periods']==1 and len(r['history'])==1 and r['status']=='insufficient_data' for r in items)
        assert next(r for r in items if r['track_id']=='x')['best_rank']==1


@case('FX20','Conflicting ranks within one capture cannot fabricate three source periods',technique='data-quality/state-transition',priority='P0',
      inputs='Same capture/track/period with ranks3,2,1',expected='No rising state; at most one observation per period')
def test_conflicting_rank_one_period(model):
    rows=[chart(rank=r,size=5) for r in [3,2,1]]
    items=model.analyze_track_lifecycle(rows)['items']
    assert all(r['periods']<=1 and r['status']=='insufficient_data' for r in items)
