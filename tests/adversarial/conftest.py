import importlib
import json
import os
from pathlib import Path
import socket
import sys
from types import SimpleNamespace

import pytest
from hypothesis import settings, HealthCheck

# Generated examples only use stateless model functions; no mutable fixture state is retained.
settings.register_profile('qa_campaign', suppress_health_check=[HealthCheck.function_scoped_fixture],
                          database=None)
settings.load_profile('qa_campaign')

WORKSPACE = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = WORKSPACE if (WORKSPACE/'serving/insights.py').exists() else WORKSPACE / 'tmp/qa-main-clean-2026-11-08'
SOURCE = Path(os.environ.get('QA_SOURCE_ROOT', str(DEFAULT_SOURCE))).resolve()
if not (SOURCE / 'serving/insights.py').exists():
    raise pytest.UsageError('Set QA_SOURCE_ROOT to the clean repository containing serving/insights.py')
for folder in [SOURCE, SOURCE/'shared', SOURCE/'crawler', SOURCE/'serving']:
    sys.path.insert(0, str(folder))


def pytest_configure(config):
    for marker in ['qa_case(id, requirement): QA traceability', 'qa_property: generated inputs',
                   'qa_integration: requires isolated real local dependencies',
                   'qa_performance: measured bounded local workload', 'qa_browser: real DOM with mocked backend']:
        config.addinivalue_line('markers', marker)


@pytest.fixture(scope='session')
def source_root():
    return SOURCE


@pytest.fixture
def model():
    module = importlib.import_module('insights')
    mutant = os.environ.get('QA_MUTANT')
    if mutant:
        import ast
        substitutions = {
            'jaccard_constant': ('len(tops[a] & tops[b]) / len(tops[a] | tops[b])', '1.0'),
            'skip_numerator': ("c['skip'] / denominator", "c['play'] / denominator"),
            'rank_score': ("(len(rows) - hit['rank'] + 1) / len(rows)", "hit['rank'] / len(rows)"),
            'placement': ("1 / log2(r['position'] + 1)", "1 / r['position']"),
            'lifecycle_direction': ("ranks[0] > ranks[1] > ranks[2]", "ranks[0] < ranks[1] < ranks[2]"),
        }
        before, after = substitutions[mutant]
        code = (SOURCE/'serving/insights.py').read_text(encoding='utf-8')
        assert code.count(before) == 1
        ns = {'__file__': str(SOURCE/'serving/insights.py')}
        exec(compile(ast.parse(code.replace(before, after)), '<in-memory-mutant>', 'exec'), ns)
        return SimpleNamespace(**ns)
    return module


@pytest.fixture(autouse=True)
def deny_external_network(monkeypatch):
    original = socket.socket.connect
    def guarded(sock, address):
        if isinstance(address, tuple) and address[0] not in ('127.0.0.1', '::1', 'localhost'):
            raise RuntimeError('QA prohibits external network: '+str(address[0]))
        return original(sock, address)
    monkeypatch.setattr(socket.socket, 'connect', guarded)


@pytest.fixture
def api_env(monkeypatch, tmp_path, source_root):
    import api
    import insights
    import httpx
    from fastapi.testclient import TestClient
    monkeypatch.setenv('INSIGHT_BACKEND', 'archive')
    monkeypatch.setattr(insights, 'ROOT', tmp_path)
    monkeypatch.setattr(api, 'STATIC_DIR', str(source_root/'serving/static'))
    monkeypatch.setattr(api, '_cache', {})
    async def get_doc(*args):
        return {'title': 'X', 'duration_s': 200, 'artists': ['Artist']}
    monkeypatch.setattr(api, 'get_doc', get_doc)
    client = TestClient(api.app, raise_server_exceptions=False)
    return SimpleNamespace(api=api, model=insights, client=client, root=tmp_path,
                           monkeypatch=monkeypatch, httpx=httpx)


@pytest.fixture
def saved_input(request):
    def remember(data):
        request.node.user_properties.append(('repro_input', json.dumps(data, ensure_ascii=False, default=str)))
        return data
    return remember


# Shared by the Phase-2 ES race regression and real-service tests.
from .test_adv_real_services import isolated_services
