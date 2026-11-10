"""Explicit opt-in, at most one bounded public feed per source plus redirects."""
from datetime import date
import math
from pathlib import Path
import sys
import time

import pytest
import requests

from .cases import case

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'shared'),str(ROOT/'crawler')]


class BoundedSession(requests.Session):
    def __init__(self):
        super().__init__()
        self.max_redirects=2
        self.calls=[]
    def get(self,url,**kwargs):
        assert len(self.calls)<1, 'Public source campaign permits one feed call per case'
        try:
            response=super().get(url,**kwargs)
        except requests.RequestException as error:
            self.calls.append({'url':url,'error':type(error).__name__})
            raise
        self.calls.append({'url':url,'status':response.status_code})
        return response


@case('PS-001','One actual public Apple VN feed satisfies native parser schema',5,kind='integration',
      services='Public Apple RSS HTTPS',expected='Real non-synthetic records, unique IDs and contiguous ranks',timeout=40)
def test_live_apple_public_feed(record_property):
    import src_apple
    capture=int(time.time()*1000)
    with BoundedSession() as session:
        try:rows=list(src_apple.market_chart(session,capture,'vn'))
        except requests.RequestException as error:
            pytest.skip('BLOCKED: public Apple transport unavailable: '+type(error).__name__)
        record_property('http_observation',str(session.calls))
        record_property('captured_ms',capture)
    records=[r for _,_,r in rows]
    assert 0<len(records)<=100
    record_property('records',len(records))
    assert len({r['platform_id'] for r in records})==len(records)
    assert {r['rank'] for r in records}==set(range(1,len(records)+1))
    assert all(r['source']=='apple_music' and r['chart_scope']=='VN' and not r.get('synthetic') for r in records)


@case('PS-002','One actual public Spotify/Kworb VN capture has real source periods',6,kind='integration',
      services='Public Kworb HTTPS',expected='Finite nonnegative counts and valid source calendar date; no fabricated period',timeout=40)
def test_live_kworb_public_feed(record_property):
    import src_spotify
    capture=int(time.time()*1000)
    with BoundedSession() as session:
        rows=list(src_spotify.crawl_kworb_daily(session,capture,'vn'))
        record_property('http_observation',str(session.calls))
        if session.calls and (session.calls[0].get('error') or session.calls[0].get('status',200)>=400):
            pytest.skip('BLOCKED: public Kworb transport/access unavailable: '+str(session.calls))
    records=[r for _,_,r in rows]
    assert records
    record_property('records',len(records))
    record_property('captured_ms',capture)
    assert len({r['platform_id'] for r in records})==len(records)
    for row in records:
        assert row['source']=='spotify' and row['data_provider']=='spotify_kworb' and row['country_code']=='VN'
        if row['source_period'] is not None:date.fromisoformat(row['source_period'])
        assert row['metric_value'] is None or math.isfinite(row['metric_value']) and row['metric_value']>=0
