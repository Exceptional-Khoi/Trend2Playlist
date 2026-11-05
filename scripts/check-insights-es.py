"""Read-only integration check after import-insight-archive.py; no cluster credentials needed."""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'serving'))
os.environ['INSIGHT_BACKEND'] = 'elasticsearch'
from fastapi.testclient import TestClient
from api import app

with TestClient(app) as client:
    for feature in ('lifecycle', 'playlists', 'countries', 'feedback'):
        response = client.get('/api/insights/' + feature, params={'days': 90, 'countries': 'VN,TH', 'as_of': '2026-11-05'})
        assert response.status_code == 200, (feature, response.text)
        data = response.json()
        assert data['data']['mode'] == 'elasticsearch'
        print(feature, 'OK', len(data['items']), 'results')
        if feature == 'countries':
            assert data['overlap'][0]['common'] == 1
            assert abs(data['overlap'][0]['jaccard'] - 1 / 99) < 1e-10
    assert client.get('/legacy').status_code == 404
