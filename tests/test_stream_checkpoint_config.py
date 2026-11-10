"""Configuration regression only; actual checkpoint recovery is verified on GKE separately."""
import ast
import os
from pathlib import Path

import pytest


def configured_recommend_location():
    """Execute the production option expression without starting Spark at import time."""
    source = Path(__file__).resolve().parents[1] / 'spark/speed_layer.py'
    module = ast.parse(source.read_text(encoding='utf-8'))
    block = next(node for node in module.body if isinstance(node, ast.If)
                 and isinstance(node.test, ast.Compare)
                 and isinstance(node.test.left, ast.Constant) and node.test.left.value == 'recommend')
    options = [node for node in ast.walk(block) if isinstance(node, ast.Call)
               and isinstance(node.func, ast.Attribute) and node.func.attr == 'option'
               and node.args and isinstance(node.args[0], ast.Constant)
               and node.args[0].value == 'checkpointLocation']
    assert len(options) == 1
    expression = ast.Expression(options[0].args[1])
    return eval(compile(expression, str(source), 'eval'), {'os': os, 'CKPT': 'hdfs://test/checkpoints'})


@pytest.mark.parametrize('configured,expected', [
    (None, 'hdfs://test/checkpoints/recommend'),
    ('', 'hdfs://test/checkpoints/recommend'),
    ('hdfs://test/checkpoints/recommend-v2-full-consume', 'hdfs://test/checkpoints/recommend-v2-full-consume'),
    ('hdfs://other/retained-state', 'hdfs://other/retained-state'),
])
def test_recommend_resumes_configured_checkpoint(monkeypatch, configured, expected):
    if configured is None:
        monkeypatch.delenv('RECOMMEND_CHECKPOINT_PATH', raising=False)
    else:
        monkeypatch.setenv('RECOMMEND_CHECKPOINT_PATH', configured)
    # A raw-ingest checkpoint belongs to another query and must not be reused here.
    monkeypatch.setenv('INSIGHT_CHECKPOINT_PATH', 'hdfs://test/checkpoints/unrelated-ingest')
    assert configured_recommend_location() == expected
