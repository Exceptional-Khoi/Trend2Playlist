import math
import pytest
from hypothesis import given, settings, HealthCheck, strategies as st

from .support import case, full_chart, event, playlist, chart, canonical_json


@case('P01','Jaccard is symmetric, bounded and agrees with independent set oracle',technique='property/differential',priority='P0',inputs='200 generated unique top-N lists',expected='Reference ratio and symmetry')
@pytest.mark.qa_property
@settings(max_examples=200,deadline=None)
@given(st.lists(st.integers(0,15),min_size=1,max_size=8,unique=True),st.lists(st.integers(0,15),min_size=1,max_size=8,unique=True))
def test_jaccard_properties(model,left,right):
    n=min(len(left),len(right));a=list(map(str,left));b=list(map(str,right))
    rows=full_chart(a)+full_chart(b,'TH')
    out=model.compare_country_tastes(rows,['VN','TH'],top_n=n)
    reverse=model.compare_country_tastes(rows,['TH','VN'],top_n=n)
    expected=len(set(a[:n])&set(b[:n]))/len(set(a[:n])|set(b[:n]))
    assert out['overlap'][0]['jaccard']==pytest.approx(expected)
    assert 0<=out['overlap'][0]['jaccard']<=1
    assert reverse['overlap'][0]['jaccard']==out['overlap'][0]['jaccard']


@case('P02','Duplicating a valid snapshot preserves a country comparison',technique='property/metamorphic',priority='P0',inputs='150 generated unique chart sets',expected='Same Jaccard after identical record duplication',risk='Duplicate-heavy ingestion invalidates complete charts')
@pytest.mark.qa_property
@settings(max_examples=150,deadline=None)
@given(st.lists(st.integers(0,9),min_size=1,max_size=5,unique=True))
def test_country_duplicate_invariance(model,ids):
    rows=full_chart(list(map(str,ids)))+full_chart(list(map(str,ids)),'TH')
    base=model.compare_country_tastes(rows,['VN','TH'],top_n=1)
    duplicate=model.compare_country_tastes(rows*2,['VN','TH'],top_n=1)
    assert duplicate['overlap']==base['overlap']


@case('P03','Feedback survives duplicate replay and reordering for consistent IDs',technique='property/metamorphic',priority='P0',inputs='200 generated action sequences',expected='Same counts, independent reference rate, rates bounded')
@pytest.mark.qa_property
@settings(max_examples=200,deadline=None)
@given(st.lists(st.sampled_from(['play','skip','like']),min_size=1,max_size=60))
def test_feedback_replay(model,actions):
    rows=[event(str(i),action=a) for i,a in enumerate(actions)]
    base=model.summarize_listening_feedback(rows)['items'][0]
    replay=model.summarize_listening_feedback((rows+rows)[::-1])['items'][0]
    assert base==replay
    denominator=actions.count('play')+actions.count('skip')
    expected=actions.count('skip')/denominator if denominator else None
    assert base['skip_rate']==expected


@case('P04','Playlist placement follows logarithmic sum for arbitrary positive positions',technique='property/differential',inputs='150 position lists',expected='Independent math.fsum oracle')
@pytest.mark.qa_property
@settings(max_examples=150,deadline=None)
@given(st.lists(st.integers(1,1000),min_size=1,max_size=12))
def test_placement_property(model,positions):
    rows=[playlist(str(i),p) for i,p in enumerate(positions)]+[chart(),chart('2026-10-02')]
    item=model.analyze_playlist_exposure(rows)['items'][0]
    assert item['placement_score']==pytest.approx(math.fsum(1/math.log2(p+1) for p in positions))


@case('P05','Equivalent NFC/NFD/case forms keep normalization stable',technique='property/metamorphic',inputs='200 Unicode text samples',expected='Idempotent, canonical-normalization invariant')
@pytest.mark.qa_property
@settings(max_examples=200,deadline=None)
@given(st.text(alphabet='abc XYZàáảãạăâêđĐ🎵日',max_size=70))
def test_unicode_normalization(source_root,text):
    import unicodedata
    from shared import textnorm
    result=textnorm.norm_text(text)
    assert textnorm.norm_text(result)==result
    assert textnorm.norm_text(unicodedata.normalize('NFD',text))==result
