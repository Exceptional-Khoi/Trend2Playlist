"""Chart, playlist and country analyses."""
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from itertools import combinations
from math import log2, isfinite
from statistics import median
import json
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]



def canonical(record):
    return json.dumps(record, sort_keys=True, ensure_ascii=False, default=str)


def timestamp(r):
    value = r.get('ts_ms')
    if value is None:
        value = r.get('crawled_ms')
    if value is not None:
        try:
            if isinstance(value, bool) or not isfinite(float(value)):
                raise ValueError('Timestamp must be finite')
            return datetime.fromtimestamp(float(value) / 1000, timezone.utc)
        except (TypeError, OverflowError, OSError) as exc:
            raise ValueError('Timestamp outside supported range') from exc
    value = r.get('crawled_at') or r.get('event_time')
    if not value:
        return None
    if not isinstance(value, str):
        raise ValueError('Timestamp must be an ISO string')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


def identity(r):
    # Platform IDs preserve Japanese/Thai titles without fuzzy cross-platform merges.
    return str(r.get('platform_id') or r.get('track_key') or r.get('track_id') or '')


def chart_group(r):
    return (r.get('data_provider') or r.get('source'), r.get('chart_id'),
            r.get('country_code') or r.get('chart_scope'), r.get('period_type'), r.get('source'))


def normalize(raw):
    if not isinstance(raw, dict):
        raise ValueError('Record must be a JSON object')
    r = dict(raw['value'] if isinstance(raw.get('value'), dict) else raw)
    if 'record_type' not in r and r.get('rank') and r.get('chart_id'):
        r['record_type'] = 'chart_entry'
    return r


def load_local():
    """Read explicit archives, not fabricated fixtures or arbitrary temporary files."""
    paths = list((ROOT / 'out').glob('country-*/records/*.jsonl'))
    paths += list((ROOT / 'out-test').glob('*/charts/*.jsonl'))
    paths += [ROOT / 'out' / f'music.{topic}.jsonl'
              for topic in ('charts', 'playlists', 'country-charts')]
    paths += [run / f'music.{topic}.jsonl'
              for run in (ROOT / 'out-test').iterdir() if run.is_dir()
              for topic in ('charts', 'playlists', 'country-charts')]
    extra = os.getenv('INSIGHT_DATA_DIR')
    if extra:
        paths += list(Path(extra).rglob('*.jsonl'))
    records, errors, seen = [], [], set()
    for path in sorted(path for path in set(paths) if path.is_file()):
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
    seen = set()
    invalid_snapshots = set()
    for r in records:
        if r.get('record_type') == 'chart_entry' and identity(r):
            # Crawler IDs are rounded buckets, not unique crawl executions.
            # Keep each capture intact before choosing the latest source period.
            capture = timestamp(r)
            snapshot = (chart_group(r), (r.get('source_period'), r.get('snapshot_id'), capture))
            rank, size = r.get('rank'), r.get('chart_size')
            if type(rank) is not int or rank < 1 or (size is not None and (type(size) is not int or size < rank)):
                invalid_snapshots.add(snapshot)
                continue
            fingerprint = canonical(r)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            snapshots[snapshot].append(r)
    # Use latest entire snapshot for a source period; never union partial snapshots.
    periods = {}
    for (group, snap), rows in snapshots.items():
        incomplete = (group, snap) in invalid_snapshots
        unique = {}
        for row in rows:
            old = unique.get(identity(row))
            if old is not None and (old.get('rank'), old.get('chart_size')) != (row.get('rank'), row.get('chart_size')):
                incomplete = True
            if old is None or canonical(row) > canonical(old):
                unique[identity(row)] = row
        rows = list(unique.values())
        if incomplete:
            rows = [dict(r, _snapshot_incomplete=True) for r in rows]
        period = rows[0].get('source_period')
        key = (group, period) if period else (group, 'unknown', str(snap))
        time = max((timestamp(r) for r in rows if timestamp(r)), default=datetime.min.replace(tzinfo=timezone.utc))
        rows.sort(key=canonical)
        candidate = (time, canonical(rows))
        if key not in periods or candidate > periods[key][0]:
            periods[key] = (candidate, rows)
    return [rows for _, rows in sorted(periods.values(), key=lambda p: p[0])]


def complete(rows):
    if not rows:
        return False
    ranks = [r.get('rank') for r in rows]
    size = rows[0].get('chart_size')
    return bool(type(size) is int and size > 0 and all(r.get('chart_size') == size and not r.get('_snapshot_incomplete') for r in rows)
                and all(type(rank) is int for rank in ranks)
                and len(set(identity(r) for r in rows)) == len(rows) == size
                and set(ranks) == set(range(1, size + 1)))


def analyze_track_lifecycle(records, track_key=None):
    groups = defaultdict(list)
    for rows in chart_snapshots(records):
        groups[chart_group(rows[0])].append(rows)
    output = []
    for group, snapshots in groups.items():
        by_track = defaultdict(list)
        for rows in snapshots:
            for r in rows:
                by_track[identity(r)].append(r)
        for track, observations in sorted(by_track.items()):
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
            output.append({'track_id': track, 'title': observations[0].get('title'), 'source': group[4] or group[0], 'data_provider': group[0],
                           'url': observations[-1].get('url'), 'artists': observations[-1].get('artists'), 'chart_id': group[1], 'country': group[2], 'best_rank': min(r['rank'] for r in observations),
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
        size = r.get('playlist_size')
        if (type(pos) is not int or pos < 1 or not r.get('playlist_id')
                or (size is not None and (type(size) is not int or size < pos))):
            continue
        key = (r.get('source'), identity(r), timestamp(r).date().isoformat(), r.get('genre_hint'))
        old = exposures[key].get(r['playlist_id'])
        if old is None or (timestamp(r), canonical(r)) > (timestamp(old), canonical(old)):
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
    def family(row):
        if row.get('chart_family'):
            return row['chart_family']
        country = row.get('country_code') or row.get('chart_scope') or ''
        return re.sub(r'(^|[_-])' + re.escape(country) + r'($|[_-])', r'\1',
                      row.get('chart_id') or '', flags=re.I).strip('_-')
    groups = defaultdict(dict)
    rejected = []
    for rows in chart_snapshots(records):
        r = rows[0]
        country = r.get('country_code') or r.get('chart_scope')
        if r.get('data_provider') != 'spotify_kworb' or r.get('source') != 'spotify' or r.get('period_type') != 'daily' or country not in countries:
            continue
        period = r.get('source_period')
        if not period or not complete(rows):
            rejected.append({'country': country, 'period': period, 'reason': 'Thiếu kỳ nguồn hoặc BXH không đủ/hạng không liên tục.'})
            continue
        groups[period].setdefault(family(r), {})[country] = rows
    compatible = {p: {f: cs for f, cs in families.items() if set(countries) <= set(cs)}
                  for p, families in groups.items()}
    available = sorted(p for p, families in compatible.items() if families)
    date = chart_date or (available[-1] if available else None)
    if date not in available:
        return {'items': [], 'overlap': [], 'date': date, 'available_dates': available, 'rejected': rejected,
                'note': 'Chưa có BXH Spotify qua Kworb đủ dữ liệu, cùng ngày cho tất cả quốc gia được chọn.'}
    selected = compatible[date][sorted(compatible[date])[0]]
    totals = {}
    for c, rows in selected.items():
        metrics = [r.get('metric_value') for r in rows]
        if any(v is not None and (type(v) not in (int, float) or not isfinite(v) or v < 0) for v in metrics):
            raise ValueError('Stream counts must be finite nonnegative numbers')
        total = None if any(v is None for v in metrics) else sum(metrics)
        if total is not None and not isfinite(total):
            raise ValueError('Stream total overflow')
        totals[c] = total
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
            total = totals[c]
            entry['countries'][c] = {'rank': hit['rank'], 'chart_size': len(rows),
                'rank_score': (len(rows) - hit['rank'] + 1) / len(rows),
                'chart_stream_share': hit['metric_value'] / total if total and hit.get('metric_value') is not None else None} if hit else None
        items.append(entry)
    return {'items': items, 'overlap': overlap, 'date': date, 'available_dates': available, 'top_n': top_n,
            'chart_sizes': {c: len(selected[c]) for c in countries}, 'rejected': rejected,
            'note': 'Spotify qua Kworb, cùng BXH ngày. Tỷ trọng stream chỉ trong BXH đã crawl. Ô trống là không có trong BXH, không phải 0 stream.'}


def song_key(row):
    artists = row.get('artists') or []
    artist = artists[0] if isinstance(artists, list) and artists else artists
    if isinstance(row.get('title'), str) and artist:
        return (row['title'].strip().casefold(), str(artist).strip().casefold())
    return (row.get('source') or '', identity(row))


def song_info(row):
    return {k: row.get(k) for k in ('title', 'artists', 'url')}


def playlist_ranking(records):
    captures = defaultdict(list)
    for row in records:
        if row.get('record_type') != 'playlist_item' or row.get('playlist_type') == 'chart_mirror':
            continue
        if row.get('playlist_id') and identity(row):
            captures[(row.get('source'), row['playlist_id'], timestamp(row))].append(row)
    memberships = defaultdict(set)
    songs = {}
    for (source, playlist, _), rows in captures.items():
        observed = len({identity(r) for r in rows})
        reported = max((r['playlist_size'] for r in rows if type(r.get('playlist_size')) is int), default=0)
        if max(observed, reported) < 5:
            continue
        for row in rows:
            key = song_key(row)
            memberships[key].add((source, playlist))
            songs[key] = song_info(row)
    return {'items': [dict(songs[k], playlist_count=len(memberships[k])) for k in
                      sorted(songs, key=lambda k: (-len(memberships[k]), k))]}


def country_winners(records, countries, days, end):
    start = end - timedelta(days=days-1)
    charts = {}
    for rows in chart_snapshots(records):
        row = rows[0]
        country = row.get('country_code') or row.get('chart_scope')
        if not re.fullmatch('[A-Z]{2}', country or '') or row.get('chart_id') in ('youtube_geo_search', 'zing_new_release'):
            continue
        if not complete(rows):
            continue
        period = row.get('source_period') or timestamp(row).date().isoformat()
        if not start.isoformat() <= period <= end.isoformat():
            continue
        key = (country, period, row.get('source'), row.get('chart_id'))
        charts[key] = rows
    scores = defaultdict(dict)
    songs = {}
    for (country, day, source, _), rows in charts.items():
        size = max(r.get('chart_size') or r['rank'] for r in rows)
        votes = scores[country, day, source]
        for r in rows:
            metric = r.get('metric_value')
            if isinstance(metric, float) and not isfinite(metric):
                raise ValueError('Non-finite chart metric')
            song = song_key(r)
            votes[song] = max(votes.get(song, 0), (size-r['rank']+1)/size)
            songs[country, song] = song_info(r)
    daily = defaultdict(list)
    for (country, day, _), votes in scores.items():
        daily[country, day].append(votes)
    totals = defaultdict(Counter)
    counts = Counter()
    for (country, _), sources in daily.items():
        counts[country] += 1
        for votes in sources:
            totals[country].update({k:v/len(sources) for k,v in votes.items()})
    items = []
    for country in sorted(totals):
        if countries and country not in countries:
            continue
        scores = totals[country]
        winner = min(scores, key=lambda k: (-scores[k], k))
        items.append(dict(songs[country, winner], country=country, score=scores[winner]/counts[country]))
    return {'items': items, 'available_countries': sorted(totals)}


def analyze(records, feature, days=30, as_of=None, **params):
    filtered = window(records, days, as_of)
    if feature == 'lifecycle':
        result = analyze_track_lifecycle(filtered, params.get('track'))
    elif feature == 'playlists':
        result = playlist_ranking(filtered)
    elif feature == 'countries':
        end = datetime.fromisoformat(as_of).date() if as_of else datetime.now(timezone.utc).date()
        result = country_winners([r for r in records if timestamp(r) and timestamp(r).date() <= end], params.get('countries'), days, end)
    else:
        raise ValueError('Unknown feature')
    result.update({'window_days': days, 'as_of': as_of or datetime.now(timezone.utc).date().isoformat(),
                   'input_records': len(filtered)})
    return result
