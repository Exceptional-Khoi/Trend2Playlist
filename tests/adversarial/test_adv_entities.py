import json
import unicodedata

import pytest

from .support import case, load_file


@case('F01','Case, accents, NFC/NFD, punctuation and whitespace normalize equivalently',technique='metamorphic',expected='Same key for orthographic aliases')
@pytest.mark.parametrize('title',['Đêm Mưa','  ĐÊM   MƯA ','Đêm-Mưa',unicodedata.normalize('NFD','Đêm Mưa')])
def test_key_aliases(title):
    from shared import textnorm
    assert textnorm.track_key(title,['Đen'])==textnorm.track_key('Đêm Mưa',['Đen'])


@case('F02','Distinct non-Latin track IDs must not collapse to unknown',technique='entity-resolution/partition',priority='P1',expected='Different keys for distinct Japanese/Thai titles',risk='False cross-language merges')
@pytest.mark.parametrize('a,b',[('夜に駆ける','青と夏'),('เพลงรัก','ความฝัน')])
def test_nonlatin_key_collision(a,b):
    from shared import textnorm
    assert textnorm.track_key(a,['Artist'])!=textnorm.track_key(b,['Artist'])


@case('F03','Original/remix/live/acoustic/cover versions retain meaningful distinctions',technique='entity-resolution/partition',expected='Distinct keys when version title differs')
@pytest.mark.parametrize('version',['Remix','Live','Acoustic','Cover','Sped Up'])
def test_version_separation(version):
    from shared import textnorm
    assert textnorm.track_key('Song',['Artist'])!=textnorm.track_key('Song ('+version+')',['Artist'])


@case('F04','Different primary artists are not merged merely by title',technique='equivalence',expected='Different identities')
def test_artist_identity():
    from shared import textnorm
    assert textnorm.track_key('Song',['A'])!=textnorm.track_key('Song',['B'])


@case('F05','Legacy artist separator x must be a word, not part of a name',technique='entity-resolution/adversarial',expected='Alex and Alexa remain different',risk='Artist names truncated to identical prefix')
def test_legacy_artist_separator(source_root):
    m=load_file(source_root/'pipeline/entity_resolution.py','qa_entity_separator')
    assert m.make_entity_key('Song','Alex')!=m.make_entity_key('Song','Alexa')


@case('F06','Legacy normalization must handle Vietnamese D-stroke aliases',technique='metamorphic',expected='Đen and Den normalize equally')
def test_legacy_d_stroke(source_root):
    m=load_file(source_root/'pipeline/entity_resolution.py','qa_entity_dstroke')
    assert m.make_entity_key('Song','Đen')==m.make_entity_key('Song','Den')


@case('F07','Legacy resolution cannot erase remix/live version boundaries',technique='entity-resolution/adversarial',expected='Different keys for original and remix',risk='All parenthesized content discarded')
def test_legacy_version_collision(source_root):
    m=load_file(source_root/'pipeline/entity_resolution.py','qa_entity_version')
    assert m.make_entity_key('Song','Artist')!=m.make_entity_key('Song (Remix)','Artist')


@case('F08','Local resolution is deterministic and preserves multiple sources',technique='filesystem/metamorphic',expected='Same canonical IDs under input permutation')
def test_resolution_order(source_root,tmp_path,monkeypatch):
    m=load_file(source_root/'pipeline/entity_resolution.py','qa_entity_order')
    monkeypatch.setattr(m,'OUT',tmp_path)
    records=[{'track_title':'Song','artist':'Artist','track_key':'spotify_chart:one','metrics':{'rank':2}},
             {'track_title':'Song','artist':'Artist','track_key':'spotify_chart:two','metrics':{'rank':1}}]
    path=tmp_path/'spotify_chart_VN.jsonl'
    def run(rows):
        path.write_text('\n'.join(json.dumps(r) for r in rows),encoding='utf-8')
        return m.resolve_entities()
    first,second=run(records),run(records[::-1])
    assert first==second


@case('F09','Song titles containing province names do not prove listener location',technique='decision-table',expected='Title province suppressed, self-identification retained otherwise')
def test_province_song_false_positive():
    from shared import textnorm
    assert not textnorm.find_province_mentions('Ha Noi mua thu dep qua','Ha Noi mua thu')
    assert ('ha-noi','self') in textnorm.find_province_mentions('Tôi ở Hà Nội đây','Song')


@case('F10','Count parser uses Vietnamese million suffix TR before trillion T',technique='boundary/differential',expected='1 TR = 1000000; 2.5K = 2500')
@pytest.mark.parametrize('text,expected',[('1 TR',1000000),('2,5K',2500),('1.234',1234),('12 N',12000)])
def test_count_units(text,expected):
    from shared import textnorm
    assert textnorm.parse_count(text)==expected
