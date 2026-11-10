"""Real logging records, synthetic credentials only; no external HTTP calls."""
import logging

import pytest


@pytest.mark.qa_case('N-SEC-001','Crawler exception logging cannot expose configured API credentials',
    priority='P0',expected='Useful failure identity retained, credential absent from message and traceback',
    technique='logging/security/fault-injection')
def test_crawler_exception_redacts_configured_key(monkeypatch,caplog):
    import common
    secret='qa-synthetic-secret-credential'
    monkeypatch.setenv('YOUTUBE_API_KEY',secret)
    common.setup_logging()
    with caplog.at_level(logging.ERROR):
        try:
            raise RuntimeError('403 for https://provider.invalid/videos?key='+secret+'&part=snippet')
        except RuntimeError:
            logging.getLogger('youtube').exception('Provider request failed')
    assert secret not in caplog.text
    assert 'Provider request failed' in caplog.text
    assert 'RuntimeError' in caplog.text


@pytest.mark.qa_case('N-SEC-002','Sensitive URL parameters and authorization headers are safe without env lookup',
    priority='P0',expected='Credential removed; provider failure remains diagnosable',technique='logging/boundary/security')
@pytest.mark.parametrize('message',[
    'https://provider.invalid?key=qa-secret-value&part=snippet',
    'https://provider.invalid?ACCESS_TOKEN=qa-secret-value',
    'Authorization: Bearer qa-secret-value'])
def test_crawler_log_transport_credentials(caplog,message):
    import common
    common.setup_logging()
    with caplog.at_level(logging.ERROR):
        logging.getLogger('crawler').error('Provider failed: %s',message)
    assert 'qa-secret-value' not in caplog.text
    assert 'Provider failed' in caplog.text
    assert '[REDACTED]' in caplog.text
