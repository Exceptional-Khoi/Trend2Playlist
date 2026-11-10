"""Regression for the new playlist, global coverage and song-specific comment requirements."""
import asyncio
import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT=Path(__file__).resolve().parents[1]
for folder in ('serving','crawler','shared'):sys.path.insert(0,str(ROOT/folder))


def test_small_unknown_and_chart_playlists_are_visible_and_deduplicated():
    m=importlib.import_module('insights')
    base=dict(record_type='playlist_item',source='spotify',platform_id='x',title='Song',artists=['Artist'],
              playlist_id='p',playlist_name='Một playlist nhỏ',playlist_size=1,position=1,crawled_at='2026-11-10T01:00:00Z')
    rows=[base,dict(base),dict(base,playlist_id='q',playlist_type='chart_mirror')]
    item,=m.playlist_ranking(rows)['items']
    assert item['playlist_count']==2
    assert {p['name'] for p in item['playlists']}=={'Một playlist nhỏ'}
    assert {p['url'] for p in item['playlists']}=={'https://open.spotify.com/playlist/p','https://open.spotify.com/playlist/q'}
    assert {p['type'] for p in item['playlists']}=={'unknown','chart_mirror'}


def test_country_map_combines_non_spotify_sources_without_requiring_same_country_set():
    m=importlib.import_module('insights')
    rows=[dict(record_type='chart_entry',source=source,platform_id=country,title=country,artists=['Artist'],
               country_code=country,chart_id=source+'_'+country,rank=1,chart_size=1,crawled_at='2026-11-10T01:00:00Z')
          for country,source in [('FR','itunes'),('US','youtube'),('VN','zing')]]
    result=m.analyze(rows,'countries',7,'2026-11-10')
    assert result['available_countries']==['FR','US','VN']
    assert result['sources']==['itunes','youtube','zing']
    assert all(i['charts'] and i['source_count']==1 for i in result['items'])


def test_itunes_schema_and_source_date(monkeypatch):
    m=importlib.import_module('src_country_public')
    payload={'feed':{'updated':{'label':'2026-11-09T20:00:00Z'},'entry':[
        {'im:name':{'label':'Été'},'im:artist':{'label':'Artiste'},'id':{'label':'https://music.apple.com/fr/song/123','attributes':{'im:id':'123'}},'im:image':[]}]}}
    response=SimpleNamespace(url='https://itunes.apple.com/fr/rss/topsongs/limit=20/json',json=lambda:payload,raise_for_status=lambda:None)
    session=SimpleNamespace(get=lambda *a,**k:response)
    _,_,row=next(m.market_itunes(session,1794268800000,'fr'))
    assert row['country_code']=='FR' and row['source']=='itunes' and row['source_period']=='2026-11-09'
    assert row['rank']==row['chart_size']==1 and row['title']=='Été'
    payload['feed']['entry']*=2
    with pytest.raises(ValueError,match='duplicate'):list(m.market_itunes(session,1794268800000,'fr'))


def test_youtube_country_html_has_real_source_period_and_search_link():
    m=importlib.import_module('src_country_public')
    text='<span>YouTube France Daily Chart - 2026/11/09</span><tbody><tr><td>1</td><td>NEW</td><td>Artist - Chanson</td><td>1,234</td></tr></tbody>'
    response=SimpleNamespace(text=text,raise_for_status=lambda:None)
    rows=list(m.market_youtube(SimpleNamespace(get=lambda *a,**k:response),1794268800000,'fr'))
    _,_,row=rows[0]
    assert row['metric_value']==1234 and row['source_period']=='2026-11-09'
    assert row['source']=='youtube' and row['country_code']=='FR' and row['id_kind']=='normalized_title_artist'
    assert 'search_query=Artist%20Chanson' in row['url']


def test_youtube_html_without_http_charset_preserves_unicode():
    import requests
    m=importlib.import_module('src_country_public')
    response=requests.Response()
    response.status_code=200
    response.encoding='ISO-8859-1'
    response._content=('<span>YouTube Vietnam Daily Chart - 2026/11/09</span><tbody>'
        '<tr><td>1</td><td>NEW</td><td>Nghệ sĩ - Đừng Quên</td><td>123</td></tr></tbody>').encode('utf8')
    _,_,row=next(m.market_youtube(SimpleNamespace(get=lambda *a,**k:response),1794268800000,'vn'))
    assert row['title']=='Đừng Quên' and row['artists']==['Nghệ sĩ']


@pytest.mark.parametrize('updated',['2026-11-10T12:00:00Z','Tue, 10 Nov 2026 12:00:00 +0000'])
def test_apple_accepts_real_rss_date_formats(updated):
    m=importlib.import_module('src_apple')
    response=SimpleNamespace(raise_for_status=lambda:None,json=lambda:{'feed':{'country':'vn','updated':updated,
        'results':[{'name':'Song','artistName':'Artist','id':'1','url':'https://music.apple.com/vn/song/1'}]}})
    _,_,row=next(m.market_chart(SimpleNamespace(get=lambda *a,**k:response),1794268800000,'vn'))
    assert row['source_period']=='2026-11-10' and row['data_provider']=='apple_music_rss'


def test_no_country_dropdown_and_no_country_query_from_dashboard():
    html=(ROOT/'serving/static/insights.html').read_text(encoding='utf8')
    js=(ROOT/'serving/static/insights.js').read_text(encoding='utf8')
    assert 'id="countries"' not in html and "$('#countries')" not in js
    assert all('data-feature="'+f+'"' in html for f in ('home','lifecycle','playlists','countries','comments'))
    assert "params.set('countries'" not in js


def test_comment_payload_has_song_context_and_generic_categories_are_removed(tmp_path,monkeypatch):
    import httpx
    t=importlib.import_module('comment_topics')
    monkeypatch.setenv('OPENAI_API_KEY','synthetic-test-key')
    monkeypatch.setenv('COMMENT_REPORT_DB',str(tmp_path/'reports.sqlite'))
    seen=[]
    async def classify(client,model,batch,previous):
        seen.append(dict(t._song_context.get()))
        return t.Groups(keywords=[t.Keyword(label='MV',variants=['MV']),t.Keyword(label='câu hát đừng quên',variants=['đừng quên'])],
                        topics=[t.Topic(label='reaction',description='Generic',ids=[0]),
                                t.Topic(label='Câu hát đừng quên gợi nỗi nhớ',description='Khán giả trích câu hát.',ids=[0])])
    monkeypatch.setattr(t,'classify',classify)
    rows=[{'comment_id':'a','text':'Câu hát đừng quên làm tôi nhớ người cũ trong MV.','title':'Bài riêng A','artists':['Nghệ sĩ A']}]
    report=asyncio.run(t.build_report(rows,{'track':'a'}))
    assert seen[0]['title']=='Bài riêng A' and seen[0]['artists']==['Nghệ sĩ A']
    assert [g['label'] for g in report['keywords']]==['câu hát đừng quên']
    assert [g['label'] for g in report['topics']]==['Câu hát đừng quên gợi nỗi nhớ']
    assert report['keywords'][0]['count']==1 and report['prompt_version'].startswith('music-comments-v2')
    assert t._song_context.get()=={}
