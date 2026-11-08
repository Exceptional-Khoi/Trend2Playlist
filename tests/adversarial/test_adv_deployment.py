import ast
from pathlib import Path
import re
import os
import ast
import shutil
import subprocess

import pytest
import yaml

from .support import case


def resources(root):
    result=[]
    for path in (root/'k8s').rglob('*.yaml'):
        for doc in yaml.safe_load_all(path.read_text(encoding='utf-8')):
            if doc:result.append((path,doc))
    return result


def podspec(resource):
    kind=resource['kind']
    if kind in ('Deployment','StatefulSet','Job'):return resource['spec']['template']['spec']
    if kind=='CronJob':return resource['spec']['jobTemplate']['spec']['template']['spec']
    return None


@case('I01','Manifests parse and agree on namespace, resource requests and limits',technique='static YAML validation',expected='All workload containers have memory limits/requests and correct namespace',environment='PyYAML only; not Kubernetes schema validation')
def test_yaml_resources(source_root):
    for path,doc in resources(source_root):
        assert doc['apiVersion'] and doc['metadata']['name']
        if doc['kind']!='Namespace':assert doc['metadata'].get('namespace')=='music',(str(path),doc['kind'])
        spec=podspec(doc)
        if spec:
            for c in spec['containers']:
                assert c.get('resources',{}).get('requests',{}).get('memory'),(str(path),c['name'])
                assert c.get('resources',{}).get('limits',{}).get('memory'),(str(path),c['name'])


@case('I02','Every referenced ConfigMap/Secret/PVC has a declarative or script producer',technique='static cross-reference',expected='No unresolved required references')
def test_references(source_root):
    docs=resources(source_root)
    known={kind:{d['metadata']['name'] for _,d in docs if d['kind']==kind} for kind in ['ConfigMap','Secret','PersistentVolumeClaim']}
    known['ConfigMap'].update(['crawler-code','spark-code','serving-code','dashboard-static','loadtest-code'])
    known['Secret'].add('music-secrets')
    missing=[]
    for path,doc in docs:
        spec=podspec(doc)
        if not spec:continue
        for volume in spec.get('volumes',[]):
            for key,kind,namekey in [('configMap','ConfigMap','name'),('secret','Secret','secretName'),('persistentVolumeClaim','PersistentVolumeClaim','claimName')]:
                if key in volume and not volume[key].get('optional') and volume[key][namekey] not in known[kind]:missing.append((str(path),volume[key]))
        for c in spec['containers']:
            for ref in c.get('envFrom',[]):
                for key,kind in [('configMapRef','ConfigMap'),('secretRef','Secret')]:
                    if key in ref and not ref[key].get('optional') and ref[key]['name'] not in known[kind]:missing.append((str(path),ref[key]))
    assert not missing


@case('I03','Spark master must disable injected conflicting service-link port variables',technique='static regression',priority='P0',expected='enableServiceLinks false or explicit safe SPARK_MASTER_PORT',risk='Spark master NumberFormatException')
def test_master_service_link_port(source_root):
    master=next(d for _,d in resources(source_root) if d['kind']=='Deployment' and d['metadata']['name']=='spark-master')
    spec=podspec(master)
    overrides={e['name']:e.get('value') for c in spec['containers'] for e in c.get('env',[])}
    assert spec.get('enableServiceLinks') is False or overrides.get('SPARK_MASTER_PORT','').isdigit()


@case('I04','Stateful query recovery honors the documented checkpoint root',technique='configuration regression',priority='P1',expected='CHECKPOINT_PATH changes root; recommend uses its own suffix',risk='Deployment rollback reuses incompatible checkpoint')
def test_recommend_checkpoint_override(source_root,monkeypatch):
    monkeypatch.setenv('CHECKPOINT_PATH','/tmp/qa-new-recovery-root')
    tree=ast.parse((source_root/'spark/jobs_common.py').read_text(encoding='utf-8'))
    assignment=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='CKPT' for t in n.targets))
    namespace={'os':os,'BASE':'/unused'}
    exec(compile(ast.Module(body=[assignment],type_ignores=[]),'<actual-checkpoint-config>','exec'),namespace)
    assert namespace['CKPT']=='/tmp/qa-new-recovery-root'
    assert 'CKPT + "/recommend"' in (source_root/'spark/speed_layer.py').read_text(encoding='utf-8')


@case('I05','Startup dependency order covers country topic and waits for driver',technique='static script contract',expected='Topic bootstrap before speed-layer; insights.py in ConfigMap; ES backend configured')
def test_deploy_data_dependencies(source_root):
    script=(source_root/'scripts/deploy.sh').read_text(encoding='utf-8')
    assert script.index('11-kafka-topics.yaml')<script.index('41-speed-layer.yaml')
    assert 'rollout status deployment/speed-layer' in script
    assert 'serving/insights.py' in (source_root/'scripts/update-code.sh').read_text(encoding='utf-8')
    api=next(d for _,d in resources(source_root) if d['kind']=='Deployment' and d['metadata']['name']=='music-api')
    assert {'name':'INSIGHT_BACKEND','value':'elasticsearch'} in podspec(api)['containers'][0]['env']


@case('I06','Bash scripts parse and use LF',technique='shell syntax/compatibility',expected='bash -n succeeds, no CRLF',environment='Git Bash or bash, no script execution')
def test_shell_syntax(source_root):
    bash=shutil.which('bash')
    git_bash=Path(r'C:\Program Files\Git\bin\bash.exe')
    if git_bash.exists():bash=str(git_bash)
    if not bash:pytest.skip('BLOCKED: Bash unavailable')
    for path in list((source_root/'scripts').glob('*.sh'))+[source_root/'spark/submit.sh']:
        assert b'\r\n' not in path.read_bytes()
        result=subprocess.run([bash,'-n',str(path)],capture_output=True,text=True,timeout=15)
        assert result.returncode==0,result.stderr


@case('I07','CI actually invokes its documented regression and syntax checks',technique='CI audit',expected='pytest, compileall and bash syntax checks present',risk='Documented validation omitted')
def test_ci_schema_validation(source_root):
    workflow=(source_root/'.github/workflows/validate.yml').read_text(encoding='utf-8')
    assert 'pytest tests' in workflow and 'compileall' in workflow and 'bash -n' in workflow


@case('I08','Production Python files compile without executing import side effects',technique='AST/compile',expected='No SyntaxError across all Python modules')
def test_python_compile(source_root):
    for folder in ['serving','crawler','crawlers','pipeline','spark','shared']:
        for path in (source_root/folder).glob('*.py'):compile(path.read_text(encoding='utf-8'),str(path),'exec')


@case('I09','Manifests satisfy Kubernetes OpenAPI JSON schema',technique='offline schema validation',expected='All manifests validate strictly against 1.31',environment='kubernetes-validate; no live API')
def test_kubernetes_schema(source_root):
    validator=pytest.importorskip('kubernetes_validate',reason='BLOCKED: offline Kubernetes schema package unavailable')
    for path,resource in resources(source_root):
        validator.validate(resource,desired_version='1.31.0',strict=True)
