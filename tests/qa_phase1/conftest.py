"""New campaign tests: offline by default; no credentials or external sockets."""
import importlib
import socket
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
for folder in (ROOT, ROOT / 'shared', ROOT / 'crawler', ROOT / 'serving'):
    sys.path.insert(0, str(folder))


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    original = socket.socket.connect
    def connect(sock, address):
        if isinstance(address, tuple) and address[0] not in ('127.0.0.1', '::1', 'localhost'):
            raise RuntimeError('Offline test attempted external network')
        return original(sock, address)
    monkeypatch.setattr(socket.socket, 'connect', connect)
    for name in ('OPENAI_API_KEY', 'YOUTUBE_API_KEY', 'SPOTIFY_CLIENT_SECRET'):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def env(monkeypatch, tmp_path):
    import httpx
    from fastapi.testclient import TestClient
    api = importlib.import_module('api')
    insights = importlib.import_module('insights')
    topics = importlib.import_module('comment_topics')
    monkeypatch.setenv('INSIGHT_BACKEND', 'archive')
    monkeypatch.setenv('INSIGHT_DATA_DIR', str(tmp_path))
    monkeypatch.setenv('COMMENT_REPORT_DB', str(tmp_path / 'report.sqlite'))
    monkeypatch.setattr(insights, 'ROOT', tmp_path)
    monkeypatch.setattr(topics, 'ROOT', tmp_path)
    monkeypatch.setattr(api, '_cache', {})
    # No context manager: lifespan installs an ES index template, not an offline read.
    client = TestClient(api.app, raise_server_exceptions=False)
    yield SimpleNamespace(api=api, model=insights, topics=topics, client=client,
                          root=tmp_path, patch=monkeypatch, httpx=httpx)
    client.close()
