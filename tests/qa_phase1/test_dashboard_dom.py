"""Actual DOM in Edge with every browser request intercepted, no live backend."""
import json
from pathlib import Path
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.qa_browser
case = pytest.mark.qa_case


@pytest.fixture(scope='module')
def browser():
    playwright = pytest.importorskip('playwright.sync_api', reason='BLOCKED: Playwright unavailable')
    edge = Path(r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe')
    if not edge.exists():
        pytest.skip('BLOCKED: Edge unavailable')
    with playwright.sync_playwright() as driver:
        instance = driver.chromium.launch(executable_path=str(edge), headless=True)
        yield instance
        instance.close()


@pytest.fixture
def ui(browser):
    page = browser.new_page(viewport={'width':1280,'height':800})
    errors = []
    page.on('pageerror', lambda error:errors.append(str(error)))
    state = {'pending':[], 'defer':None, 'fail':None, 'report_fail':False, 'size':1}
    attack = '<img src=x onerror="window.qaAttack=1">'
    def route(request):
        path = urlparse(request.request.url).path
        if path == '/':
            return request.fulfill(content_type='text/html', body=(ROOT/'serving/static/insights.html').read_bytes())
        if path.startswith('/static/'):
            file = ROOT/'serving/static'/path.rsplit('/',1)[1]
            content_type = {'.js':'text/javascript','.css':'text/css','.json':'application/json'}.get(file.suffix,'text/plain')
            return request.fulfill(content_type=content_type, body=file.read_bytes())
        if path.startswith('/api/'):
            feature = path.rsplit('/',1)[1]
            if feature == state['defer']:
                state['pending'].append(request)
                return
            if feature == state['fail']:
                return request.fulfill(status=503, json={'detail':'offline failure'})
            if feature == 'tracks':
                return request.fulfill(json={'items':[{'key':'x','title':'Song'}], 'as_of':'2026-10-01'})
            if feature == 'report':
                if state['report_fail']:
                    return request.fulfill(status=502, json={'detail':'Controlled error'})
                return request.fulfill(json={'summary':attack, 'sample_size':1, 'start_ms':1790812800000,'end_ms':1790899200000,
                    'sample_sorts':['recent'], 'topics':[{'label':'Hay','count':1,'share':1,
                    'evidence':[{'excerpt':attack}]}], 'keywords':[]})
            if feature == 'countries':
                payload = {'available_countries':['VN'], 'items':[{'country':'VN','title':'Country song', 'url':'javascript:alert(1)'}]}
            elif feature == 'playlists':
                payload = {'items':[{'title':f'Playlist song {i}', 'artists':['Artist'], 'playlist_count':2,
                                     'url':'javascript:alert(1)'} for i in range(state['size'])]}
            else:
                payload = {'items':[{'title':attack, 'source':'spotify', 'chart_id':'daily_vn','country':'VN',
                                     'best_rank':1,'status':'rising','history':[]}]}
            return request.fulfill(json=payload)
        request.abort()
    page.route('**/*', route)
    # Direct lifecycle URL isolates row/security assertions from the new Home view.
    page.goto('http://qa.invalid/#lifecycle', wait_until='domcontentloaded')
    page.wait_for_function("document.querySelectorAll('tbody tr').length===1")
    yield page, state, errors
    page.close()


@case('N-UI-001', 'Every active feature renders and untrusted strings remain text', priority='P0',
      expected='Four features, rendered evidence, no injected DOM or unsafe links', technique='real-DOM/e2e-fixtures')
@pytest.mark.parametrize('mobile', [False, True])
def test_four_features_and_evidence(ui, mobile):
    page, state, errors = ui
    if mobile:
        page.set_viewport_size({'width':390,'height':844})
    assert page.locator('tbody').inner_text().find('<img') >= 0
    assert page.locator('#results img').count() == 0
    page.locator('[data-feature=playlists]').click()
    page.wait_for_function("document.querySelector('tbody')?.textContent.includes('Playlist song')")
    assert page.locator('a[href^="javascript:"]').count() == 0
    page.locator('[data-feature=countries]').click()
    page.wait_for_selector('svg path.has-data')
    path = page.locator('svg path.has-data').first
    path.focus()
    path.press('Enter')
    page.wait_for_function("document.querySelector('.country-code')?.textContent==='VN'")
    assert page.locator('#countries').count()==0
    assert page.locator('svg path.has-data').count()==1
    page.locator('[data-feature=comments]').click()
    page.locator('.report-button').click()
    page.wait_for_selector('.topic-card')
    page.locator('.topic-evidence summary').first.click()
    assert page.locator('#results img').count() == 0
    assert page.evaluate('window.qaAttack') is None
    assert errors == []


@case('N-UI-002', 'Late responses cannot replace the current feature', priority='P1',
      expected='Countries remain visible after deferred playlist response completes', technique='real-DOM/async-race')
def test_stale_feature_response(ui):
    page, state, errors = ui
    state['defer'] = 'playlists'
    # Wait for the intercepted request itself, not an arbitrary timing delay.
    with page.expect_request('**/api/insights/playlists?*'):
        page.locator('[data-feature=playlists]').click()
    page.locator('[data-feature=countries]').click()
    page.wait_for_selector('svg path.has-data')
    assert state['pending']
    for route in state['pending']:
        route.fulfill(json={'items':[{'title':'STALE','playlist_count':99}]})
    page.evaluate('()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))')
    assert 'STALE' not in page.locator('#results').inner_text()
    assert page.locator('svg').count() == 1
    assert errors == []


@case('N-UI-003', 'Comment report error reenables controls and permits retry', priority='P1',
      expected='Visible error, enabled button, successful next report', technique='real-DOM/recovery')
def test_comment_retry_after_failure(ui):
    page, state, errors = ui
    state['report_fail'] = True
    page.locator('[data-feature=comments]').click()
    button = page.locator('.report-button')
    button.click()
    page.get_by_text('Controlled error', exact=True).wait_for()
    assert button.is_enabled()
    assert page.locator('#results select').is_enabled()
    state['report_fail'] = False
    button.click()
    page.locator('.topic-card').wait_for()
    assert errors == []


@case('N-UI-004', 'Pagination and search limit rendered rows while preserving all matches', priority='P2',
      expected='50 rows per page, 121 total, precise search', technique='real-DOM/boundary')
def test_table_search_pagination(ui):
    page, state, errors = ui
    state['size'] = 121
    page.locator('[data-feature=playlists]').click()
    page.wait_for_function("document.querySelectorAll('tbody tr').length===50")
    page.get_by_role('button', name='Sau →', exact=True).click()
    assert 'Playlist song 50' in page.locator('tbody').inner_text()
    page.get_by_role('button', name='Sau →', exact=True).click()
    assert page.locator('tbody tr').count() == 21
    page.get_by_placeholder('Tìm trong bảng…').fill('Playlist song 120')
    assert page.locator('tbody tr').count() == 1
    assert errors == []
