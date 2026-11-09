"""Real image compaction in owned HDFS, independently of Spark checkpoints."""
import copy
import re

import yaml

from .cases import case
from .safety import poll, sha256, validate_scope
from .stack import LABEL


def checkpoint_resources(scope, source):
    validate_scope(scope)
    content = (source / 'k8s/21-hdfs-checkpoint.yaml').read_text(encoding='utf-8')
    content = content.replace('.music.svc.cluster.local', '.' + scope['namespace'] + '.svc.cluster.local')
    result = []
    for item in yaml.safe_load_all(content):
        obj = copy.deepcopy(item)
        obj['metadata']['namespace'] = scope['namespace']
        obj['metadata'].setdefault('labels', {})[LABEL] = scope['run_id']
        if obj['kind'] == 'ConfigMap':
            # Match the owned NameNode's accelerated test timing. Neither the
            # primary-image assertion nor production upload checks are disabled.
            xml = obj['data']['hdfs-site.xml']
            for old, new in (('900', '10'), ('200000', '10'), ('60', '5')):
                xml = xml.replace('<value>' + old + '</value>', '<value>' + new + '</value>')
            obj['data']['hdfs-site.xml'] = xml
        if obj['kind'] == 'Deployment':
            obj['spec']['template']['metadata'].setdefault('labels', {})[LABEL] = scope['run_id']
            obj['spec']['template']['spec'].update(serviceAccountName='qa-runner')
        result.append(obj)
    return result


@case('CH-001', 'SecondaryNameNode advances primary fsimage without losing namespace entries', 100,
      kind='integration', services='HDFS,SecondaryNameNode,Kubernetes', timeout=300,
      expected='Primary checkpoint transaction advances; all created directories survive; no pod restart')
def test_secondary_checkpoint_preserves_namespace(stack):
    path = '/qa/' + stack.run_id + '/checkpoint-proof'
    def image_ids():
        output = stack.exec('hdfs-namenode', ['ls', '/data/nn/current'], timeout=30)
        return sorted(int(s) for s in re.findall(r'^fsimage_(\d+)$', output, re.M))
    before = image_ids()
    assert before, 'Primary must already have a valid fsimage'
    paths = [path + '/entry-' + str(i) for i in range(16)]
    stack.exec('hdfs-namenode', ['hdfs', 'dfs', '-mkdir', '-p', *paths], timeout=60)
    for obj in checkpoint_resources(stack.scope, stack.source):
        stack.create(obj)
    stack.ready('Deployment', 'hdfs-checkpoint', timeout=180)
    after = poll(image_ids, lambda ids: bool(ids) and max(ids) > max(before),
                 timeout=180, interval=3, guard=stack.guard)
    listing = stack.exec('hdfs-namenode', ['hdfs', 'dfs', '-ls', path], timeout=45)
    actual = {line.split()[-1] for line in listing.splitlines() if line.startswith('d')}
    assert set(paths) == actual, 'Checkpoint must preserve every namespace entry'
    pod = stack.pod('hdfs-checkpoint')
    assert all(c.get('ready') and c.get('restartCount', 0) == 0 for c in pod['status']['containerStatuses'])
    logs = stack.kube('-n', stack.namespace, 'logs', pod['metadata']['name'], '--tail=200')
    stack.save('hdfs_checkpoint/proof.json', {'before_image_ids': before, 'after_image_ids': after,
        'expected_paths': paths, 'actual_paths': sorted(actual), 'pod_uid': pod['metadata']['uid'],
        'image_id': pod['status']['containerStatuses'][0]['imageID'], 'logs': logs,
        'source_manifest_sha256': sha256((stack.source/'k8s/21-hdfs-checkpoint.yaml').read_bytes()),
        'controlled_test_policy': 'Both owned nodes use10s period/10transactions/5s check; production policy not modified',
        'deployed_config': stack.get('ConfigMap', 'hdfs-checkpoint-config')['data'],
        'deployed_template': stack.get('Deployment', 'hdfs-checkpoint')['spec']['template']})
    stack.trace('primary_image_checkpoint', 'PASS', {'before': before, 'after': after, 'preserved_entries': len(paths)})
