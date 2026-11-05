"""Import preserved raw archives into the dedicated insight index; never VN score indices."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import requests
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'serving'))
from insights import load_local


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--es', default='http://127.0.0.1:9200')
    args = ap.parse_args()
    records, metadata = load_local()
    if metadata['errors']:
        raise RuntimeError(f"Archive contains {len(metadata['errors'])} invalid lines; inspect before import")
    for start in range(0, len(records), 500):
        lines = []
        for r in records[start:start + 500]:
            value = json.dumps(r, sort_keys=True, ensure_ascii=False)
            doc_id = 'archive-' + hashlib.sha256(value.encode()).hexdigest()
            lines.extend([json.dumps({'index': {'_index': 'music-insight-raw', '_id': doc_id}}),
                          json.dumps({'value': value}, ensure_ascii=False)])
        response = requests.post(args.es.rstrip('/') + '/_bulk', data=('\n'.join(lines)+'\n').encode(),
                                 headers={'Content-Type': 'application/x-ndjson'}, timeout=60)
        response.raise_for_status()
        if response.json().get('errors'):
            raise RuntimeError('Bulk import has errors; inspect Elasticsearch mappings and retry (IDs are stable)')
    print(f'Imported {len(records)} records; original dates and source labels preserved.')


if __name__ == '__main__':
    main()
