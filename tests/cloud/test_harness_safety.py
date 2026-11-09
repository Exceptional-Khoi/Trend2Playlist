"""Offline guard tests test our campaign's safety, not old application logic."""
import copy
from pathlib import Path

import pytest

from .safety import SafetyError,owned,quantity,validate_scope
from .stack import LABEL,resources
from .test_hdfs_checkpoint import checkpoint_resources


def scope():
    return {'project':'qa-example-project','cluster':'music','location':'example-zone',
       'context':'gke_qa-example-project_example-zone_music','namespace':'music-e2e-20261108-guard',
       'run_id':'20261108-guard','approved_existing_nodes':True}


@pytest.mark.parametrize('change',[{'namespace':'music'},{'namespace':'default'},{'approved_existing_nodes':False},
    {'context':'gke_wrong_project'},{'run_id':''},{'run_id':'../../escape'}])
def test_scope_rejects_unsafe_target(change):
    target=scope();target.update(change)
    with pytest.raises(SafetyError):validate_scope(target)


def test_cleanup_refuses_foreign_resource():
    with pytest.raises(SafetyError):owned({'metadata':{'uid':'x','namespace':'music','labels':{LABEL:'other'}}},'my-run','music-e2e-20261108-guard')


def test_render_never_creates_billable_storage_or_external_service():
    source=Path(__file__).resolve().parents[2]
    manifests=resources(scope(),source)+checkpoint_resources(scope(),source)
    assert not any(m['kind']=='PersistentVolumeClaim' or m.get('spec',{}).get('volumeClaimTemplates') for m in manifests)
    assert all(m['spec'].get('type','ClusterIP')=='ClusterIP' for m in manifests if m['kind']=='Service')
    assert all(m['metadata']['labels'][LABEL]==scope()['run_id'] for m in manifests)
    assert all(m['metadata']['namespace']==scope()['namespace'] for m in manifests if m['kind']!='Namespace')
    checkpoint_config=next(m for m in manifests if m['kind']=='ConfigMap' and m['metadata']['name']=='hdfs-checkpoint-config')
    assert '.music.svc.cluster.local' not in str(checkpoint_config['data'])
    assert ('.'+scope()['namespace']+'.svc.cluster.local') in str(checkpoint_config['data'])
    import xml.etree.ElementTree as ET
    def policy(xml):return {p.findtext('name'):p.findtext('value') for p in ET.fromstring(xml).findall('property')}
    primary=next(m for m in manifests if m['kind']=='ConfigMap' and m['metadata']['name']=='hdfs-config')
    for cm in (primary,checkpoint_config):
        values=policy(cm['data']['hdfs-site.xml'])
        assert values['dfs.namenode.checkpoint.period']=='10' and values['dfs.namenode.checkpoint.txns']=='10'
        assert values.get('dfs.namenode.checkpoint.upload.check','true')=='true'


@pytest.mark.parametrize('value,expected',[('100m',.1),('100000000n',.1),('100000u',.1),('2',2)])
def test_cpu_units_cannot_bypass_guard(value,expected):assert quantity(value,'cpu')==pytest.approx(expected)
