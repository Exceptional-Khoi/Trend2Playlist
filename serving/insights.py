"""Four evidence-based analyses specified in docs/07. No inferred listening duration."""
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from itertools import combinations
from math import log2
from statistics import median
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def timestamp(r):
    value = r.get('ts_ms') or r.get('crawled_ms')
    if value is not None:
        return datetime.fromtimestamp(float(value) / 1000, timezone.utc)
    value = r.get('crawled_at') or r.get('event_time')
    return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc) if value else None


def identity(r):
    # Platform IDs preserve Japanese/Thai titles without fuzzy cross-platform merges.
    return str(r.get('platform_id') or r.get('track_key') or r.get('track_id') or '')


def chart_group(r):
    return (r.get('data_provider') or r.get('source'), r.get('chart_id'),
            r.get('country_code') or r.get('chart_scope'), r.get('period_type'))


def normalize(raw):
    r = dict(raw['value'] if isinstance(raw.get('value'), dict) else raw)
    if 'record_type' not in r:
        if r.get('action'):
            r['record_type'] = 'event'
        elif r.get('rank') and r.get('chart_id'):
            r['record_type'] = 'chart_entry'
    return r


def load_local():
    """Read explicit archives, not fabricated fixtures or arbitrary temporary files."""
    paths = list((ROOT / 'out').glob('country-*/records/*.jsonl'))
    paths += list((ROOT / 'out-test').glob('*/charts/*.jsonl'))
    paths += list((ROOT / 'out').glob('music.*.jsonl'))
    paths += list((ROOT / 'out-test').glob('*/music.*.jsonl'))
    extra = os.getenv('INSIGHT_DATA_DIR')
    if extra:
        paths += list(Path(extra).rglob('*.jsonl'))
    if (ROOT / 'output/user_events.jsonl').exists():
        paths.append(ROOT / 'output/user_events.jsonl')
    local_events = ROOT / 'out/insight-web-events.jsonl'
    if local_events.exists():
        paths.append(local_events)
    records, errors, seen = [], [], set()
    for path in sorted(set(paths)):
        for n, line in enumerate(path.read_text(encoding='utf-8-sig').splitlines(), 1):
            if not line.strip():
                continue
            try:
                r = normalize(json.loads(line))
                key = json.dumps(r, sort_keys=True, ensure_ascii=False)
                if key not in seen:
                    records.append(r)
                    seen.add(key)
            except (ValueError, TypeError) as exc:
                errors.append(f'{path.name}:{n}: {type(exc).__name__}')
    return records, {'mode': 'archive', 'files': len(set(paths)), 'errors': errors,
                     'note': 'Dữ liệu lưu trữ; không tự đổi ngày cũ thành hôm nay.'}


def window(records, days, as_of=None):
    end = datetime.fromisoformat(as_of).replace(tzinfo=timezone.utc) + timedelta(days=1) if as_of else datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return [r for r in records if timestamp(r) and start <= timestamp(r) < end]


def chart_snapshots(records):
    snapshots = defaultdict(list)
    for r in records:
        if r.get('record_type') == 'chart_entry' and identity(r):
            snapshots[(chart_group(r), (r.get('source_period'), r.get('snapshot_id') or r.get('crawled_at') or r.get('crawled_ms')))].append(r)
    # Use latest entire snapshot for a source period; never union partial snapshots.
    periods = {}
    for (group, snap), rows in snapshots.items():
        period = rows[0].get('source_period')
        key = (group, period) if period else (group, 'unknown', str(snap))
        time = max((timestamp(r) for r in rows if timestamp(r)), default=datetime.min.replace(tzinfo=timezone.utc))
        if key not in periods or time > periods[key][0]:
            periods[key] = (time, rows)
    return [rows for _, rows in periods.values()]


def complete(rows):
    ranks = [r.get('rank') for r in rows]
    size = rows[0].get('chart_size') or max(ranks, default=0)
    return bool(size and len(set(identity(r) for r in rows)) == len(rows) == size
                and set(ranks) == set(range(1, size + 1)))


def analyze_track_lifecycle(records, track_key=None):
    groups = defaultdict(list)
    for rows in chart_snapshots(records):
        groups[chart_group(rows[0])].append(rows)
    output = []
    for group, snapshots in groups.items():
        tracks = {identity(r) for rows in snapshots for r in rows}
        for track in sorted(tracks):
            observations = [r for rows in snapshots for r in rows if identity(r) == track]
            if track_key and track_key not in (track, observations[0].get('track_key')):
                continue
            known = sorted([r for r in observations if r.get('source_period')], key=lambda r: r['source_period'])
            status, reason = 'insufficient_data', 'Chưa có ít nhất 3 kỳ nguồn xác định; thời điểm crawl không thay cho kỳ BXH.'
            if len(known) >= 3:
                ranks = [r['rank'] for r in known[-3:]]
                if ranks[0] > ranks[1] > ranks[2]:
                    status, reason = 'rising', 'Hạng tốt lên trong hai khoảng chuyển kỳ liên tiếp.'
                elif ranks[0] < ranks[1] < ranks[2]:
                    status, reason = 'falling', 'Hạng xấu đi trong hai khoảng chuyển kỳ liên tiếp.'
                elif max(ranks) - min(ranks) <= 3:
                    status, reason = 'stable', 'Ba kỳ gần nhất dao động không quá 3 hạng.'
                else:
                    status, reason = 'fluctuating', 'Hạng biến động, chưa có hướng liên tiếp.'
            if known:
                later = [rows for rows in snapshots if (rows[0].get('source_period') or '') > known[-1]['source_period']]
                if later:
                    latest = max(later, key=lambda rows: rows[0]['source_period'])
                    if complete(latest) and all(identity(r) != track for r in latest):
                        status, reason = 'left_chart', 'Không xuất hiện ở kỳ tiếp theo đã crawl đủ hạng; chỉ kết luận rời BXH này.'
            times = sorted(timestamp(r) for r in observations if timestamp(r))
            output.append({'track_id': track, 'title': observations[0].get('title'), 'source': group[0],
                           'chart_id': group[1], 'country': group[2], 'best_rank': min(r['rank'] for r in observations),
                           'periods': len(known), 'observations': len(observations), 'status': status, 'reason': reason,
                           'first_observed': times[0].isoformat() if times else None,
                           'tracking_days': (times[-1] - times[0]).days if times else 0,
                           'history': [{'period': r.get('source_period'), 'rank': r['rank'],
                                        'crawled_at': timestamp(r).isoformat() if timestamp(r) else None} for r in sorted(observations, key=lambda r: str(r.get('source_period') or ''))]})
    return {'items': output, 'note': 'Lần đầu quan sát trong cửa sổ được chọn không phải ngày phát hành. Luật ổn định: dao động ≤3 hạng trong 3 kỳ.'}


def analyze_playlist_exposure(records, genre=None):
    exposures = defaultdict(dict)
    unknown, mirrors = 0, 0
    for r in records:
        if r.get('record_type') != 'playlist_item' or not timestamp(r) or not identity(r):
            continue
        kind = r.get('playlist_type', 'unknown')
        if kind == 'chart_mirror':
            mirrors += 1
            continue
        if kind != 'editorial':
            unknown += 1
            continue
        if genre and genre != r.get('genre_hint'):
            continue
        pos = r.get('position')
        if not isinstance(pos, int) or pos < 1:
            continue
        key = (r.get('source'), identity(r), timestamp(r).date().isoformat(), r.get('genre_hint'))
        old = exposures[key].get(r['playlist_id'])
        if old is None or timestamp(r) > timestamp(old):
            exposures[key][r['playlist_id']] = r
    charts = [r for rows in chart_snapshots(records) for r in rows if r.get('source_period') and r.get('period_type') == 'daily']
    items = []
    for (source, track, day, gen), lists in exposures.items():
        base = [r for r in charts if r.get('source') == source and identity(r) == track and r['source_period'] == day]
        for b in base:
            for lag in (1, 3, 7):
                target = (datetime.fromisoformat(day) + timedelta(days=lag)).date().isoformat()
                after = [r for r in charts if chart_group(r) == chart_group(b) and identity(r) == track and r['source_period'] == target]
                if not after:
                    continue
                items.append({'track_id': track, 'title': b.get('title'), 'source': source, 'chart_id': b['chart_id'],
                              'genre': gen, 'date': day, 'lag_days': lag, 'playlist_count': len(lists),
                              'placement_score': sum(1 / log2(r['position'] + 1) for r in lists.values()),
                              'rank_before': b['rank'], 'rank_after': after[0]['rank'],
                              'rank_improvement': b['rank'] - after[0]['rank']})
    return {'items': items, 'observations': len(items), 'editorial_exposures': len(exposures),
            'excluded_unknown_type': unknown, 'excluded_chart_mirror': mirrors,
            'median_improvement': median(x['rank_improvement'] for x in items) if items else None,
            'note': 'Chỉ playlist đã xác nhận editorial; so cùng BXH ngày ở mốc 1/3/7 ngày. Mối liên hệ không chứng minh quan hệ nhân quả. Thiếu kỳ trước/sau thì không tính.'}


def compare_country_tastes(records, countries, chart_date=None, top_n=50):
    groups = defaultdict(dict)
    rejected = []
    for rows in chart_snapshots(records):
        r = rows[0]
        country = r.get('country_code') or r.get('chart_scope')
        if r.get('data_provider') != 'spotify_kworb' or r.get('period_type') != 'daily' or country not in countries:
            continue
        period = r.get('source_period')
        if not period or not complete(rows):
            rejected.append({'country': country, 'period': period, 'reason': 'Thiếu kỳ nguồn hoặc BXH không đủ/hạng không liên tục.'})
            continue
        groups[period][country] = rows
    available = sorted(p for p, cs in groups.items() if set(countries) <= set(cs))
    date = chart_date or (available[-1] if available else None)
    if date not in available:
        return {'items': [], 'overlap': [], 'date': date, 'available_dates': available, 'rejected': rejected,
                'note': 'Chưa có BXH Spotify qua Kworb đủ dữ liệu, cùng ngày cho tất cả quốc gia được chọn.'}
    selected = groups[date]
    tops = {c: {identity(r) for r in selected[c] if r['rank'] <= top_n} for c in countries}
    if any(len(tops[c]) < top_n for c in countries):
        return {'items': [], 'overlap': [], 'date': date, 'note': 'BXH không đủ top N yêu cầu.'}
    overlap = [{'countries': [a, b], 'common': len(tops[a] & tops[b]),
                'jaccard': len(tops[a] & tops[b]) / len(tops[a] | tops[b])} for a, b in combinations(countries, 2)]
    ids = set.union(*tops.values())
    catalog = {identity(r): r for c in countries for r in selected[c]}
    items = []
    for track in sorted(ids):
        entry = {'track_id': track, 'title': catalog[track].get('title'), 'countries': {}}
        for c in countries:
            rows = selected[c]
            hit = next((r for r in rows if identity(r) == track), None)
            total = sum(r.get('metric_value') or 0 for r in rows)
            entry['countries'][c] = {'rank': hit['rank'], 'chart_size': len(rows),
                'rank_score': (len(rows) - hit['rank'] + 1) / len(rows),
                'chart_stream_share': hit['metric_value'] / total if total and hit.get('metric_value') is not None else None} if hit else None
        items.append(entry)
    return {'items': items, 'overlap': overlap, 'date': date, 'available_dates': available, 'top_n': top_n,
            'chart_sizes': {c: len(selected[c]) for c in countries}, 'rejected': rejected,
            'note': 'Spotify qua Kworb, cùng BXH ngày. Tỷ trọng stream chỉ trong BXH đã crawl. Ô trống là không có trong BXH, không phải 0 stream.'}


def summarize_listening_feedback(records, source='web', group_by='track'):
    groups = defaultdict(list)
    seen, source_counts = set(), Counter()
    for r in records:
        if r.get('action') not in ('play', 'skip', 'like'):
            continue
        if not r.get('event_id') or r['event_id'] in seen:
            continue
        seen.add(r['event_id'])
        src = r.get('source') or 'unknown'
        source_counts[src] += 1
        if src != source:
            continue
        key = {'track': r.get('track_key') or r.get('track_id'), 'genre': r.get('genre'),
               'province': r.get('province_code') or r.get('province'),
               'hour': (timestamp(r) + timedelta(hours=7)).strftime('%Y-%m-%d %H:00') if timestamp(r) else None}[group_by]
        groups[key or 'unknown'].append(r)
    items = []
    for key, rows in groups.items():
        c = Counter(r['action'] for r in rows)
        denominator = c['play'] + c['skip']
        items.append({'group': key, 'title': rows[0].get('title') if group_by == 'track' else key,
                      'play': c['play'], 'skip': c['skip'], 'like': c['like'],
                      'skip_rate': c['skip'] / denominator if denominator else None,
                      'users': len({r['user_id'] for r in rows if r.get('user_id')}), 'events': len(rows)})
    return {'items': sorted(items, key=lambda x: -x['events']), 'source': source, 'source_counts': dict(source_counts),
            'note': 'Đếm thao tác, không đếm phiên nghe. skip/(play+skip); không tính thời lượng/hoàn tất. User ID chưa chứng minh người thật riêng biệt. Nguồn thiếu nhãn được giữ unknown.'}


def analyze(records, feature, days=30, as_of=None, **params):
    filtered = window(records, days, as_of)
    if feature == 'lifecycle':
        result = analyze_track_lifecycle(filtered, params.get('track'))
    elif feature == 'playlists':
        result = analyze_playlist_exposure(filtered, params.get('genre'))
    elif feature == 'countries':
        result = compare_country_tastes(filtered, params.get('countries', ['VN', 'TH']), params.get('chart_date'), params.get('top_n', 50))
    elif feature == 'feedback':
        result = summarize_listening_feedback(filtered, params.get('source', 'web'), params.get('group_by', 'track'))
        catalog = {r.get('track_key') or identity(r): r for r in filtered if r.get('record_type') == 'chart_entry'}
        result['catalog'] = [{'key': k, 'title': r.get('title') or k} for k, r in list(catalog.items())[:500] if k]
    else:
        raise ValueError('Unknown feature')
    result.update({'window_days': days, 'as_of': as_of or datetime.now(timezone.utc).date().isoformat(),
                   'input_records': len(filtered)})
    return result
