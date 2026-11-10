"""Regression from real GKE EROFS: report SQLite cannot live in a code ConfigMap."""
from pathlib import Path, PurePosixPath
import sys

import pytest
import yaml

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'tests'))


@pytest.mark.qa_case('N-DEP-001','Mounted API report database has explicit writable bounded storage',
    priority='P0',expected='COMMENT_REPORT_DB is below a writable mounted volume, never the code ConfigMap',
    technique='deployment-contract/real-cloud-regression')
@pytest.mark.parametrize('profile',['production','owned-test'])
def test_comment_database_is_writable_volume(profile):
    if profile=='production':
        docs=list(yaml.safe_load_all((ROOT/'k8s/60-serving.yaml').read_text(encoding='utf-8')))
    else:
        from cloud.stack import resources
        scope={'project':'qa-example','cluster':'music','location':'example','context':'gke_qa-example_example_music',
            'namespace':'music-e2e-qa20261110-storage','run_id':'qa20261110-storage','approved_existing_nodes':True}
        docs=resources(scope,ROOT)
    api=next(r for r in docs if r['kind']=='Deployment' and r['metadata']['name']=='music-api')
    pod=api['spec']['template']['spec'];container=pod['containers'][0]
    values={e['name']:e.get('value') for e in container.get('env',[])}
    assert values.get('COMMENT_REPORT_DB'), 'Default source-relative path is read-only when code is a ConfigMap'
    path=PurePosixPath(values['COMMENT_REPORT_DB'])
    assert path.is_absolute()
    writable={v['name'] for v in pod['volumes'] if 'emptyDir' in v or 'persistentVolumeClaim' in v}
    mounts=[m for m in container['volumeMounts'] if m['name'] in writable and not m.get('readOnly')]
    assert any(path.is_relative_to(PurePosixPath(m['mountPath'])) for m in mounts)
    assert not path.is_relative_to(PurePosixPath('/app'))
