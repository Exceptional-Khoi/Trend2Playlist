#!/usr/bin/env python3
"""Opt-in bounded runner. Never creates clusters, disks, node pools or IAM roles."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback
import uuid

SOURCE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(SOURCE/'tests'))


def version():
    def git(*args):
        return subprocess.run(['git','-C',str(SOURCE),*args],capture_output=True,text=True,timeout=45).stdout.strip()
    files={str(p.relative_to(SOURCE)).replace('\\','/'):hashlib.sha256(p.read_bytes()).hexdigest()
      for folder in ('crawler','crawlers','serving','spark','shared','pipeline','schemas','k8s')
      for p in (SOURCE/folder).rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.suffix in ('.py','.yaml','.json','.sh','.txt','.html','.js','.css')}
    return {'branch':git('branch','--show-current'),'commit':git('rev-parse','HEAD'),
      'status':git('status','--short'),'application_hashes':files,
      'remote_main':git('ls-remote','https://github.com/Exceptional-Khoi/Trend2Playlist.git','refs/heads/main'),
      'remote_dev':git('ls-remote','https://github.com/Exceptional-Khoi/Trend2Playlist.git','refs/heads/dev')}


class Results:
    def __init__(self,report,harness=None,readonly=None):self.report=report;self.harness=harness;self.readonly=readonly;self.items={};self.started=time.monotonic()
    def pytest_configure(self,config):config.cloud_harness=self.harness;config.cloud_readonly=self.readonly
    def pytest_runtest_logreport(self,report):
        item=self.items.setdefault(report.nodeid,{'nodeid':report.nodeid,'outcome':'not_run','duration':0})
        item['duration']+=report.duration
        if report.when=='call' or report.failed or report.skipped:
            item['outcome']=report.outcome
            if report.longrepr:item['details']=str(report.longrepr)
        self.flush()
    def flush(self):
        self.report.mkdir(parents=True,exist_ok=True)
        (self.report/'results.json').write_text(json.dumps({'elapsed_seconds':time.monotonic()-self.started,
            'tests':list(self.items.values())},ensure_ascii=False,indent=2),encoding='utf-8')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=['local','gke-readonly','gke-e2e','gke-reliability','gke-performance','gke-all'])
    parser.add_argument('--scope',type=Path,help='Approved project/context/unique namespace/run ID JSON; required for GKE')
    parser.add_argument('--report-dir',type=Path,default=SOURCE/'test_reports/cloud_e2e')
    parser.add_argument('--allow-destructive',action='store_true',help='Only owned isolated fault scenarios')
    parser.add_argument('--case-id',action='append',default=[],help='Recheck selected independent case IDs in a fresh owned stack')
    parser.add_argument('--collect-only',action='store_true')
    parser.add_argument('--prepare-only',action='store_true',help='Readiness and cleanup, no test execution')
    args=parser.parse_args();os.chdir(SOURCE)
    import pytest
    report=args.report_dir.resolve();report.mkdir(parents=True,exist_ok=True)
    if args.collect_only:
        return int(pytest.main(['tests/cloud','--collect-only','-q','--strict-markers','-p','no:cacheprovider']))
    if args.mode=='local':
        recorder=Results(report)
        paths=[str(p) for p in (SOURCE/'tests').glob('test_*.py')]+['tests/adversarial','tests/cloud/test_harness_safety.py']
        return int(pytest.main(paths+['-ra','--junitxml='+str(report/'junit.xml'),'-p','no:cacheprovider'],plugins=[recorder]))
    if args.scope is None:parser.error('GKE modes require an explicit approved --scope JSON')
    from cloud.harness import Harness
    from cloud.safety import sanitize
    scope=json.loads(args.scope.read_text(encoding='utf-8-sig'))
    before=version();scope['source_version']=before
    if args.mode=='gke-readonly':
        from cloud.readonly import ReadOnly
        observer=ReadOnly(scope,SOURCE,report,'music');recorder=Results(report,readonly=observer)
        try:
            observer.save('local_source.json',before);observer.observe_open()
            return int(pytest.main(['tests/cloud/test_live_readonly.py','-ra','--cloud-mode=all',
                '--cloud-scope='+str(args.scope.resolve()),'-p','no:cacheprovider',
                '--junitxml='+str(report/'junit.xml')],plugins=[recorder]))
        finally:observer.close();recorder.flush()
    h=Harness(scope,SOURCE,report);recorder=Results(report,h);outcome={'status':'PREPARING','phase':2}
    exit_code=2
    try:
        h.save('source_before.json',before);h.open();h.guard(force=True);h.prepare()
        if args.prepare_only:outcome['status']='READY';exit_code=0
        else:
            mode=args.mode.removeprefix('gke-')
            options=['tests/cloud','-ra','--strict-markers','-p','no:cacheprovider',
                '--cloud-mode='+mode,'--cloud-scope='+str(args.scope.resolve()),'--junitxml='+str(report/'junit.xml')]
            if args.allow_destructive:options.append('--allow-destructive')
            for identifier in args.case_id:options.extend(['--cloud-case-id',identifier])
            exit_code=int(pytest.main(options,plugins=[recorder]));outcome['status']='EXECUTED';outcome['pytest_exit_code']=exit_code
    except Exception as exc:
        outcome.update(status='BLOCKED',reason=sanitize(str(exc)),traceback=sanitize(traceback.format_exc()))
        print(json.dumps(outcome,ensure_ascii=False),flush=True)
    finally:
        try:h.cleanup()
        except Exception as exc:outcome['cleanup_error']=sanitize(traceback.format_exc())
        after=version();h.save('source_after.json',after)
        outcome['application_unchanged']=before['application_hashes']==after['application_hashes']
        h.save('execution_state.json',outcome);recorder.flush()
    return exit_code


if __name__=='__main__':raise SystemExit(main())
