"""Reproducible inventory, frozen baseline, execution capture and offline network gate."""
import ast
from collections import Counter
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
ART = ROOT / 'qa_artifacts'
PRODUCTION = ('crawler/','crawlers/','serving/','spark/','shared/','pipeline/','local/','scripts/','k8s/','schemas/')


def git(*args):
    return subprocess.run(['git', '-C', str(ROOT), *args], capture_output=True, text=True,
                          encoding='utf-8', timeout=60, check=True).stdout


def save(name, value):
    ART.mkdir(exist_ok=True)
    target=ART/name
    temporary=target.with_suffix(target.suffix+'.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    for attempt in range(6):
        try:
            temporary.replace(target)
            break
        except PermissionError:
            # Windows readers/antivirus may hold a brief handle without DELETE
            # sharing. Keep atomic publication; never suppress permanent errors.
            if attempt==5:raise
            time.sleep(.02*(attempt+1))


def source_hashes():
    return {f:hashlib.sha256((ROOT/f).read_bytes()).hexdigest() for f in git('ls-files').splitlines()
            if f.startswith(PRODUCTION) and '__pycache__' not in f}


def inventory():
    items = []
    for path in git('ls-files').splitlines():
        if not path.startswith(PRODUCTION):
            continue
        file = ROOT/path
        if file.suffix not in ('.py','.js','.html','.css','.yaml','.sh','.ps1','.json','.txt'):
            continue
        code = file.read_text(encoding='utf-8-sig', errors='replace')
        item = {'path':path, 'lines':len(code.splitlines()), 'sha256':hashlib.sha256(file.read_bytes()).hexdigest(),
                'urls':sorted(set(re.findall(r'https?://[^\s\"\'<>]+', code)))}
        if file.suffix == '.py':
            tree = ast.parse(code)
            item.update(functions=[], classes=[], env=[], routes=[], handlers=[], schemas=[], imports=[])
            for node in ast.walk(tree):
                if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)):
                    item['functions'].append({'name':node.name, 'line':node.lineno,
                        'signature':ast.unparse(node.args), 'doc':ast.get_docstring(node),
                        'conditions':sum(isinstance(n,(ast.If,ast.IfExp,ast.BoolOp)) for n in ast.walk(node))})
                    for dec in node.decorator_list:
                        if isinstance(dec,ast.Call) and isinstance(dec.func,ast.Attribute) and dec.func.attr in ('get','post','put','delete','patch'):
                            item['routes'].append({'function':node.name,'method':dec.func.attr.upper(),
                                'path':ast.literal_eval(dec.args[0]) if dec.args and isinstance(dec.args[0],ast.Constant) else ast.unparse(dec)})
                if isinstance(node,ast.ClassDef):
                    item['classes'].append({'name':node.name,'bases':[ast.unparse(n) for n in node.bases]})
                if isinstance(node,ast.ExceptHandler):
                    item['handlers'].append({'line':node.lineno,'exception':ast.unparse(node.type) if node.type else 'bare'})
                if isinstance(node,ast.Import):
                    item['imports'].extend(n.name for n in node.names)
                if isinstance(node,ast.ImportFrom):
                    item['imports'].append(node.module)
                if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and node.func.attr in ('getenv','get'):
                    if ('os.' in ast.unparse(node.func) and node.args and isinstance(node.args[0],ast.Constant)):
                        item['env'].append({'name':node.args[0].value,'default':ast.unparse(node.args[1]) if len(node.args)>1 else None})
                if isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id in ('StructField','Field'):
                    item['schemas'].append({'line':node.lineno,'definition':ast.unparse(node)})
            item['topics_indexes_checkpoints'] = sorted(set(re.findall(r'[\"\']((?:music[.-]|[^\"\'\n]*checkpoint)[^\"\'\n]*)[\"\']',code)))
        if file.suffix in ('.html','.js'):
            item['active_features'] = sorted(set(re.findall(r'data-feature=[\"\']([^\"\']+)', code)))
        items.append(item)
    save('implementation_inventory.json', items)
    save('baseline.json', {'commit':git('rev-parse','HEAD').strip(),'branch':git('branch','--show-current').strip(),
         'status':git('status','--short'), 'production_hashes':source_hashes(),
         'source_of_truth':'Current tracked implementation; no README/docs used as behavioral specification'})
    print(json.dumps({'production_files':len(items),'functions':sum(len(x.get('functions',[])) for x in items),
                      'routes':sum(len(x.get('routes',[])) for x in items)}))


class Recorder:
    def __init__(self, name, freeze=False):
        self.name, self.freeze, self.items = name, freeze, {}
        self.started = time.monotonic()

    def pytest_collection_finish(self, session):
        manifest = []
        for item in session.items:
            marker = item.get_closest_marker('qa_case') or item.get_closest_marker('cloud_case')
            kwargs = marker.kwargs if marker else {}
            args = marker.args if marker else ()
            row = {'nodeid':item.nodeid,'id':kwargs.get('id') or (args[0] if args else item.nodeid),
                'requirement':kwargs.get('requirement') or (args[1] if len(args)>1 else item.originalname or item.name),
                'priority':kwargs.get('priority','P1'),'oracle':kwargs.get('expected','Assertions in test implementation; not all independently audited'),
                'technique':kwargs.get('technique','existing regression'), 'mode':kwargs.get('environment',kwargs.get('services','offline')),
                'inputs':kwargs.get('inputs','See parametrization/fixture'), 'test_path':str(item.path.relative_to(ROOT)).replace('\\','/'),
                'markers':[m.name for m in item.iter_markers()]}
            if 'cloud' in row['markers'] or 'cloud_case' in row['markers']:
                row['mode']='cloud'
            manifest.append(row)
        if self.freeze:
            save('phase1_test_manifest.json', manifest)
            tests = {str(p.relative_to(ROOT)).replace('\\','/'):hashlib.sha256(p.read_bytes()).hexdigest()
                     for p in (ROOT/'tests').rglob('*.py') if '__pycache__' not in p.parts}
            save('phase1_frozen_tests.json',tests)
            baseline=json.loads((ART/'baseline.json').read_text(encoding='utf-8'))
            if source_hashes()!=baseline['production_hashes']:
                raise RuntimeError('Phase 1 modified production code')
            with (ART/'phase1_coverage_matrix.csv').open('w',newline='',encoding='utf-8') as stream:
                writer=csv.DictWriter(stream,fieldnames=list(manifest[0]))
                writer.writeheader();writer.writerows(manifest)
        elif self.name=='final_collection.json':
            save('final_manifest.json',manifest)

    def pytest_runtest_logreport(self, report):
        row=self.items.setdefault(report.nodeid,{'nodeid':report.nodeid,'status':'ERROR','duration':0})
        row['duration']+=report.duration
        if report.failed:
            row['status']='FAIL' if report.when=='call' else 'ERROR'
            row['details']=str(report.longrepr)
        elif report.skipped:
            row['details']=str(report.longrepr)
            row['status']='BLOCKED' if any(s in row['details'] for s in ('BLOCKED','INCONCLUSIVE')) else 'SKIPPED_VALID'
        elif report.when=='call':
            row['status']='PASS'
        save(self.name,{'elapsed_seconds':time.monotonic()-self.started,'counts':dict(Counter(x['status'] for x in self.items.values())),
                        'tests':list(self.items.values())})


def main():
    os.chdir(ROOT)
    mode=sys.argv[1]
    if mode=='inventory':
        inventory();return 0
    import pytest
    if mode=='freeze':
        return int(pytest.main(['tests','--collect-only','-q','-p','no:cacheprovider','--strict-markers'],
                               plugins=[Recorder('collection.json', True)]))
    # Whole offline session denies external sockets, not just tests with local fixtures.
    original=socket.socket.connect
    def connect(sock,address):
        if isinstance(address,tuple) and address[0] not in ('127.0.0.1','::1','localhost'):
            raise RuntimeError('Offline session forbids external network')
        return original(sock,address)
    socket.socket.connect=connect
    for name in ('OPENAI_API_KEY','YOUTUBE_API_KEY','SPOTIFY_CLIENT_SECRET','GOOGLE_APPLICATION_CREDENTIALS'):
        os.environ.pop(name,None)
    os.environ['QA_REPORT_DIR']=str(ART)
    os.environ['QA_SOURCE_ROOT']=str(ROOT)
    args=sys.argv[2:] or ['tests']
    return int(pytest.main(args+['-ra','--strict-markers','-p','no:cacheprovider'],plugins=[Recorder(mode+'.json')]))


if __name__=='__main__':
    raise SystemExit(main())
