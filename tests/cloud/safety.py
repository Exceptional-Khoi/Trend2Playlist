import hashlib
import json
import re
import time


class SafetyError(RuntimeError):
    pass


class Blocked(RuntimeError):
    pass


def quantity(value,kind='memory'):
    value=str(value or '0')
    if kind=='cpu':
        for suffix,multiplier in [('n',1e-9),('u',1e-6),('m',1e-3)]:
            if value.endswith(suffix):return float(value[:-1])*multiplier
        return float(value)
    units={'Ki':1024,'Mi':1024**2,'Gi':1024**3,'Ti':1024**4,'K':1000,'M':1000**2,'G':1000**3}
    for suffix,multiplier in units.items():
        if value.endswith(suffix):return float(value[:-len(suffix)])*multiplier
    return float(value)


def sha256(value):
    if not isinstance(value,bytes):value=json.dumps(value,sort_keys=True,ensure_ascii=False).encode('utf-8')
    return hashlib.sha256(value).hexdigest()


def owned(obj,run_id,namespace=None):
    meta=obj.get('metadata',{})
    if meta.get('labels',{}).get('qa.trend2playlist/run')!=run_id:
        raise SafetyError('Refusing resource without matching test-run label')
    if namespace and meta.get('namespace')!=namespace:
        raise SafetyError('Resource namespace differs from approved test namespace')
    return meta['uid']


def validate_scope(scope):
    required=('project','cluster','location','context','namespace','run_id')
    if not all(scope.get(k) for k in required):raise SafetyError('Explicit project/cluster/context/namespace/run_id required')
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{7,44}',scope['run_id']):raise SafetyError('Unsafe run ID')
    expected='gke_'+scope['project']+'_'+scope['location']+'_'+scope['cluster']
    if scope['context']!=expected:raise SafetyError('Context does not identify the approved cluster')
    if not re.fullmatch(r'music-e2e-[a-z0-9-]{8,45}',scope['namespace']):raise SafetyError('Unsafe namespace')
    if scope['namespace'] in ('music','default','kube-system'):raise SafetyError('Shared namespace forbidden')
    if not scope.get('approved_existing_nodes'):raise SafetyError('Existing-node use has not been authorized')
    return scope


def sanitize(text):
    # Preserve useful stack traces, remove credential-like values and auth headers.
    text=re.sub(r'(?i)(authorization\s*[:=]\s*)([^\r\n]+)',r'\1[REDACTED]',str(text))
    text=re.sub(r'(?i)((?:access_token|refresh_token|api[_-]?key|client_secret|password)\s*[=:]\s*)[^\s,;]+',r'\1[REDACTED]',text)
    text=re.sub(r'ya29\.[A-Za-z0-9._-]+','[REDACTED_TOKEN]',text)
    return text


def poll(fn,predicate=lambda value:bool(value),timeout=90,interval=2,guard=None):
    deadline=time.monotonic()+timeout;last=None
    while True:
        if guard:guard()
        last=fn()
        if predicate(last):return last
        remaining=deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('Eventual condition timed out; last='+sanitize(repr(last))[:1200])
        # Bounded condition polling, never used as a substitute for readiness.
        import threading
        threading.Event().wait(min(interval,remaining))
