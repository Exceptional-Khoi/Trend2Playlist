import ast
import copy
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest


def case(identifier, requirement, *, technique='boundary', priority='P1',
         inputs='See parametrization and fixture in test', expected='Invariant holds',
         risk='Incorrect analysis or unhandled input', environment='offline'):
    return pytest.mark.qa_case(identifier, requirement, technique=technique,
                               priority=priority, inputs=inputs, expected=expected,
                               risk=risk, environment=environment)


def chart(day='2026-10-01', rank=1, track='x', country='VN', size=2, **extra):
    row = dict(record_type='chart_entry', source='spotify', data_provider='spotify_kworb',
               chart_id='daily_' + country, country_code=country, period_type='daily',
               source_period=day, snapshot_id=f'{country}-{day}',
               crawled_at=day+'T12:00:00+00:00', platform_id=track, track_key=track,
               title=track, artists=['Artist'], rank=rank, chart_size=size, metric_value=100)
    row.update(extra)
    return row


def full_chart(tracks, country='VN', day='2026-10-01', **extra):
    return [chart(day, i, track, country, len(tracks), **extra)
            for i, track in enumerate(tracks, 1)]


def playlist(pid='p', position=1, day='2026-10-01', track='x', **extra):
    row = dict(record_type='playlist_item', playlist_id=pid, playlist_type='editorial',
               source='spotify', platform_id=track, track_key=track, title=track,
               artists=['Artist'], position=position, genre_hint='pop',
               crawled_at=day+'T12:00:00+00:00')
    row.update(extra)
    return row


def event(eid='e', action='play', source='web', **extra):
    row = dict(record_type='event', event_id=eid, user_id='u', track_key='x', title='X',
               province_code='ha-noi', genre='pop', source=source, action=action,
               ts_ms=1790845200000)  # 2026-10-01 09:00 UTC
    row.update(extra)
    return row


def load_file(path, alias):
    spec = importlib.util.spec_from_file_location(alias, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def isolated_function(path, name, globals_):
    """Execute the actual function AST, with dependency doubles; not a Spark integration."""
    tree = ast.parse(Path(path).read_text(encoding='utf-8'))
    fn = next(n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name)
    module = ast.Module(body=[copy.deepcopy(fn)], type_ignores=[])
    ns = dict(globals_)
    exec(compile(ast.fix_missing_locations(module), str(path), 'exec'), ns)
    return ns[name]


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


class Response:
    def __init__(self, body=None, status=200, text=None):
        self.body = body
        self.status_code = status
        self.text = text if text is not None else json.dumps(body)
        self.encoding = 'utf-8'

    def json(self):
        if isinstance(self.body, Exception):
            raise self.body
        return self.body

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(str(self.status_code), response=self)


class Session:
    def __init__(self, response):
        self.response = response
        self.headers = {}
        self.calls = []

    def get(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def kworb_html(ids=('aaa', 'bbb'), ranks=None, date='2026/10/01'):
    ranks = ranks or range(1, len(ids)+1)
    rows = []
    for rank, track in zip(ranks, ids):
        cells = [str(rank), '=', f'<a href="../track/{track}.html">Song {track}</a>'
                 '<a href="../artist/artist.html">Artist</a>', '0', '0', '0', '100', '0', '0', '0', '1000']
        rows.append('<tr>'+''.join('<td>'+c+'</td>' for c in cells)+'</tr>')
    return f'<span class="pagetitle">{date}</span><tbody>'+''.join(rows)+'</tbody>'
