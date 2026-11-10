"""Owned-namespace orchestration and checkpoint evidence, never shared fault injection."""
import concurrent.futures
import datetime
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from .safety import SafetyError,Blocked,owned,poll,quantity,sanitize,sha256,validate_scope
from .stack import LABEL,SPARK_IMAGE,resources

PATHS={'Namespace':'/api/v1/namespaces','ConfigMap':'/api/v1/namespaces/{ns}/configmaps',
 'Service':'/api/v1/namespaces/{ns}/services','ServiceAccount':'/api/v1/namespaces/{ns}/serviceaccounts',
 'Pod':'/api/v1/namespaces/{ns}/pods','ResourceQuota':'/api/v1/namespaces/{ns}/resourcequotas',
 'LimitRange':'/api/v1/namespaces/{ns}/limitranges','Deployment':'/apis/apps/v1/namespaces/{ns}/deployments',
 'StatefulSet':'/apis/apps/v1/namespaces/{ns}/statefulsets','Job':'/apis/batch/v1/namespaces/{ns}/jobs',
 'NetworkPolicy':'/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies'}


class ObservationUnavailable(RuntimeError):
    """Transient read failure; never a malformed committed-file exemption."""


def process(args,timeout=45,input=None):
    p=subprocess.run(args,input=input,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=timeout)
    if p.returncode:raise RuntimeError(sanitize(p.stderr or p.stdout)[-3000:])
    return p.stdout


class Harness:
    def __init__(self,scope,source,report):
        self.scope=validate_scope(scope);self.source=Path(source);self.report=Path(report)
        self.report.mkdir(parents=True,exist_ok=True);self.journal=[];self.processes=[];self.current='setup'
        self.namespace=scope['namespace'];self.run_id=scope['run_id'];self.namespace_uid=None
        self.started=time.monotonic();self.deadline=self.started+scope.get('max_runtime_seconds',3600)
        self.proxy=None;self.agent_url=None;self.last_guard=0;self.baseline=None;self.abort_reason=None
        self.evidence_file=self.report/'checkpoints.jsonl';self.counter=0

    def save(self,name,data):
        path=self.report/name;path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(sanitize(json.dumps(data,ensure_ascii=False,indent=2,default=str)),encoding='utf-8')

    def trace(self,stage,status,data):
        record={'case':self.current,'stage':stage,'status':status,'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'elapsed_seconds':time.monotonic()-self.started,'data':data}
        with self.evidence_file.open('a',encoding='utf-8') as out:out.write(sanitize(json.dumps(record,ensure_ascii=False,default=str))+'\n')

    def kube(self,*args,timeout=45,input=None):
        return process(['kubectl','--context='+self.scope['context'],'--request-timeout=20s',*args],timeout,input)

    def _spawn(self,args,name):
        log=self.report/(name+'.log');handle=log.open('w',encoding='utf-8')
        kwargs={'creationflags':getattr(subprocess,'CREATE_NO_WINDOW',0)} if os.name=='nt' else {}
        p=subprocess.Popen(args,stdout=handle,stderr=subprocess.STDOUT,text=True,**kwargs)
        self.processes.append((p,handle));return p,log

    def open(self):
        # Explicit cluster address verification; never relies on ambient gcloud project.
        cluster=json.loads(process(['gcloud.cmd','container','clusters','describe',self.scope['cluster'],
            '--zone='+self.scope['location'],'--project='+self.scope['project'],'--format=json(name,location,endpoint,privateClusterConfig,autoscaling)']))
        server=self.kube('config','view','--minify','-o','jsonpath={.clusters[0].cluster.server}').strip()
        endpoints=[cluster.get('endpoint'),cluster.get('privateClusterConfig',{}).get('privateEndpoint')]
        if server.removeprefix('https://') not in endpoints:raise SafetyError('Kube API endpoint does not match approved GKE cluster')
        pools=json.loads(process(['gcloud.cmd','container','node-pools','list','--cluster='+self.scope['cluster'],
            '--zone='+self.scope['location'],'--project='+self.scope['project'],'--format=json(name,autoscaling)']))
        if any(p.get('autoscaling',{}).get('enabled') for p in pools) or cluster.get('autoscaling',{}).get('enableNodeAutoprovisioning'):
            raise Blocked('Autoscaling enabled: cannot guarantee no additional billable nodes')
        p,log=self._spawn(['kubectl','--context='+self.scope['context'],'proxy','--address=127.0.0.1','--port=0'],'kube-proxy')
        def proxy_address():
            text=log.read_text(encoding='utf-8',errors='replace')
            match=re.search(r'127\.0\.0\.1:(\d+)',text)
            if p.poll() is not None:raise RuntimeError(sanitize(text)[-1000:])
            return 'http://127.0.0.1:'+match.group(1) if match else None
        self.proxy=poll(proxy_address,timeout=30,interval=.2)
        self.baseline=self._baseline()
        if len(self.baseline['nodes'])!=self.scope.get('expected_nodes',3):raise SafetyError('Node count differs from approved capacity')
        self._headroom()
        self.save('baseline_before.json',self.baseline)
        self.save('approved_scope.json',self.scope)
        self.save('cluster_identity.json',{'server':server,'cluster':cluster,'node_pools':pools})

    def request(self,method,path,body=None,content_type='application/json',timeout=30):
        req=urllib.request.Request(self.proxy+path,method=method,
            data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':content_type})
        with urllib.request.urlopen(req,timeout=timeout) as response:
            content=response.read()
        return json.loads(content) if content else {}

    def _baseline(self):
        nodes=self.request('GET','/api/v1/nodes')['items']
        pods=self.request('GET','/api/v1/namespaces/music/pods')['items']
        controllers={}
        for kind,resource in [('Deployment','deployments'),('StatefulSet','statefulsets')]:
            for obj in self.request('GET','/apis/apps/v1/namespaces/music/'+resource)['items']:
                controllers[kind+'/'+obj['metadata']['name']]={'uid':obj['metadata']['uid'],
                    'template_hash':sha256(obj['spec']['template']),'desired':obj['spec'].get('replicas',1),
                    'ready':obj.get('status',{}).get('readyReplicas',0)}
        hpas={}
        for obj in self.request('GET','/apis/autoscaling/v2/namespaces/music/horizontalpodautoscalers')['items']:
            ref=obj['spec']['scaleTargetRef'];key=ref['kind']+'/'+ref['name']
            hpas[key]={'uid':obj['metadata']['uid'],'spec_hash':sha256(obj['spec']),
                       'minimum':obj['spec'].get('minReplicas',1)}
        return {'nodes':[{'name':n['metadata']['name'],'allocatable':n['status']['allocatable'],
                           'conditions':n['status']['conditions']} for n in nodes],
                'controllers':controllers,'hpas':hpas,
                'workloads':[{'name':p['metadata']['name'],'uid':p['metadata']['uid'],'phase':p['status'].get('phase'),
                    'app':p['metadata'].get('labels',{}).get('app'),
                    'ready':bool(p['status'].get('containerStatuses')) and all(c.get('ready') for c in p['status'].get('containerStatuses',[])),
                    'deleting':bool(p['metadata'].get('deletionTimestamp')),
                    'restarts':sum(c.get('restartCount',0) for c in p['status'].get('containerStatuses',[]))}
                    for p in pods if p['status'].get('phase')=='Running'
                    and not any(o.get('kind')=='Job' for o in p['metadata'].get('ownerReferences',[]))]}

    def _headroom(self):
        pods=self.request('GET','/api/v1/pods')['items']
        used_cpu=used_memory=0
        for pod in pods:
            if pod['status'].get('phase') in ('Succeeded','Failed'):continue
            containers=pod['spec'].get('containers',[])
            init=pod['spec'].get('initContainers',[])
            used_cpu+=max(sum(quantity(c.get('resources',{}).get('requests',{}).get('cpu',0),'cpu') for c in containers),
                max([quantity(c.get('resources',{}).get('requests',{}).get('cpu',0),'cpu') for c in init] or [0]))
            used_memory+=max(sum(quantity(c.get('resources',{}).get('requests',{}).get('memory',0)) for c in containers),
                max([quantity(c.get('resources',{}).get('requests',{}).get('memory',0)) for c in init] or [0]))
        total_cpu=sum(quantity(n['allocatable']['cpu'],'cpu') for n in self.baseline['nodes'])
        total_memory=sum(quantity(n['allocatable']['memory']) for n in self.baseline['nodes'])
        self.save('headroom.json',{'allocatable_cpu':total_cpu,'requested_cpu':used_cpu,'allocatable_memory':total_memory,
                                   'requested_memory':used_memory,'test_request_budget_cpu':3,'test_request_budget_memory':7*1024**3})
        if total_cpu-used_cpu<2.2 or total_memory-used_memory<7*1024**3:
            raise Blocked('Insufficient existing node requests headroom; cloud expansion forbidden')

    def guard(self,force=False):
        if self.abort_reason:raise SafetyError(self.abort_reason)
        if time.monotonic()>self.deadline:raise SafetyError('Campaign runtime budget exceeded')
        if not force and time.monotonic()-self.last_guard<12:return
        self.last_guard=time.monotonic()
        current=self._baseline()
        if {n['name'] for n in current['nodes']}!={n['name'] for n in self.baseline['nodes']}:
            self.abort_reason='Node inventory changed during campaign';raise SafetyError(self.abort_reason)
        old={w['name']:w for w in self.baseline['workloads']}
        def healthy_known_hpa(pod):
            key='Deployment/'+str(pod.get('app'))
            before_hpa=self.baseline.get('hpas',{}).get(key)
            now_hpa=current.get('hpas',{}).get(key)
            before_controller=self.baseline.get('controllers',{}).get(key)
            now_controller=current.get('controllers',{}).get(key)
            return bool(before_hpa and before_hpa==now_hpa and before_controller and now_controller
                and before_controller['uid']==now_controller['uid']
                and before_controller['template_hash']==now_controller['template_hash']
                and now_controller['ready']>=max(1,now_hpa['minimum']))
        current_names={w['name'] for w in current['workloads']}
        turnover=[name for name,w in old.items() if w['ready'] and name not in current_names and healthy_known_hpa(w)]
        if turnover:self.trace('healthy_hpa_pod_turnover','OBSERVED',{'pods':turnover,'controllers':current['controllers']})
        missing=[name for name,w in old.items() if w['ready'] and name not in current_names and not healthy_known_hpa(w)]
        if missing:
            self.abort_reason='Original running workload disappeared: '+','.join(missing)
            raise SafetyError(self.abort_reason)
        for w in current['workloads']:
            if w['name'] in old and old[w['name']]['ready'] and not w['ready'] and not (w.get('deleting') and healthy_known_hpa(w)):
                self.abort_reason='Original workload readiness degraded: '+w['name'];raise SafetyError(self.abort_reason)
            if w['name'] in old and w['restarts']>old[w['name']]['restarts']:
                self.abort_reason='Original workload restarted: '+w['name'];raise SafetyError(self.abort_reason)
        try:
            metrics=self.request('GET','/apis/metrics.k8s.io/v1beta1/nodes')
            alloc={n['name']:n['allocatable'] for n in current['nodes']}
            for m in metrics.get('items',[]):
                a=alloc[m['metadata']['name']]
                memory=quantity(m['usage']['memory'])/quantity(a['memory'])
                cpu=quantity(m['usage']['cpu'],'cpu')/quantity(a['cpu'],'cpu')
                if memory>.90 or cpu>.95:
                    self.trace('resource_guard','FAIL',{'node':m['metadata']['name'],
                        'cpu_fraction':cpu,'memory_fraction':memory,'usage':m['usage']})
                    self.abort_reason='Node safety threshold exceeded';raise SafetyError(self.abort_reason)
            self.trace('resource_sample','OBSERVED',{'node_metrics':metrics})
        except urllib.error.HTTPError as exc:
            self.trace('resource_sample','NOT MEASURED',{'http_status':exc.code})

    def collection(self,kind):return PATHS[kind].format(ns=self.namespace)
    def get(self,kind,name):return self.request('GET',self.collection(kind)+'/'+name)
    def list(self,kind):return self.request('GET',self.collection(kind))['items']
    def verify_namespace(self):
        obj=self.get('Namespace',self.namespace)
        uid=owned(obj,self.run_id)
        if self.namespace_uid and uid!=self.namespace_uid:raise SafetyError('Namespace UID changed')
        return obj

    def create(self,obj):
        self.guard()
        kind=obj['kind']
        if kind not in PATHS:raise SafetyError('Resource kind outside test allowlist')
        if obj['metadata'].get('labels',{}).get(LABEL)!=self.run_id:raise SafetyError('Missing run label')
        if kind=='Namespace':
            if obj['metadata']['name']!=self.namespace:raise SafetyError('Wrong namespace create')
        else:
            self.verify_namespace()
            if obj['metadata'].get('namespace')!=self.namespace:raise SafetyError('Namespace mismatch')
        created=self.request('POST',self.collection(kind),obj)
        uid=owned(created,self.run_id,None if kind=='Namespace' else self.namespace)
        if kind=='Namespace':self.namespace_uid=uid
        self.journal.append({'kind':kind,'name':created['metadata']['name'],'uid':uid})
        self.save('resource_journal.json',self.journal)
        return created

    def delete(self,kind,name):
        self.verify_namespace()
        obj=self.get(kind,name);uid=owned(obj,self.run_id,None if kind=='Namespace' else self.namespace)
        return self.request('DELETE',self.collection(kind)+'/'+name,{'apiVersion':'v1','kind':'DeleteOptions',
            'preconditions':{'uid':uid},'propagationPolicy':'Foreground'})

    def patch(self,kind,name,body):
        self.guard(force=True);self.verify_namespace();obj=self.get(kind,name);owned(obj,self.run_id,self.namespace)
        if body.get('spec',{}).get('replicas',0)>3:raise SafetyError('Replica expansion forbidden')
        return self.request('PATCH',self.collection(kind)+'/'+name,body,content_type='application/merge-patch+json')

    def ready(self,kind,name,timeout=600):
        def check():
            obj=self.get(kind,name);status=obj.get('status',{});spec=obj.get('spec',{})
            return obj if status.get('readyReplicas',0)==spec.get('replicas',1) and status.get('observedGeneration',0)>=obj['metadata']['generation'] else None
        return poll(check,timeout=timeout,interval=3,guard=self.guard)

    def pod(self,app):
        values=[p for p in self.list('Pod') if p['metadata'].get('labels',{}).get('app')==app and not p['metadata'].get('deletionTimestamp')]
        values.sort(key=lambda p:p['metadata']['creationTimestamp'],reverse=True)
        if not values:raise Blocked('Owned pod missing: '+app)
        owned(values[0],self.run_id,self.namespace);return values[0]

    def exec(self,app,argv,timeout=45):
        self.verify_namespace();pod=self.pod(app)
        return self.kube('exec','-n',self.namespace,pod['metadata']['name'],'--',*argv,timeout=timeout)

    def prepare(self):
        manifests=resources(self.scope,self.source)
        self.save('manifests.json',{'apiVersion':'v1','kind':'List','items':manifests})
        staged={}
        for obj in manifests:
            if obj['kind'] in ('Deployment','StatefulSet'):
                staged[(obj['kind'],obj['metadata']['name'])]=obj
                continue
            self.create(obj)
        for kind,name in [('StatefulSet','kafka'),('StatefulSet','hdfs-namenode'),('StatefulSet','hdfs-datanode'),
                          ('Deployment','elasticsearch'),('Deployment','es-gate'),('Deployment','spark-master'),
                          ('Deployment','spark-worker'),('Deployment','qa-agent'),('Deployment','music-api')]:
            # Stagger real readiness, rather than booting every JVM at once on
            # shared physical nodes. Resource safety thresholds are unchanged.
            self.create(staged[(kind,name)])
            print('WAIT '+kind+'/'+name,flush=True);self.ready(kind,name,timeout=600)
        p,log=self._spawn(['kubectl','--context='+self.scope['context'],'-n',self.namespace,'port-forward',
            'pod/'+self.pod('qa-agent')['metadata']['name'],':8899','--address=127.0.0.1'],'agent-forward')
        def address():
            text=log.read_text(encoding='utf-8',errors='replace');match=re.search(r'127\.0\.0\.1:(\d+)',text)
            if p.poll() is not None:raise RuntimeError(sanitize(text)[-1000:])
            return 'http://127.0.0.1:'+match.group(1) if match else None
        self.agent_url=poll(address,timeout=30,interval=.2)
        self.trace('topics','PASS',self.rpc('topics_init',timeout=90))
        self.create(staged[('Deployment','speed-layer')])
        self.ready('Deployment','speed-layer',timeout=900)
        poll(lambda:self.kube('logs','-n',self.namespace,'deployment/speed-layer','--tail=100'),
             lambda text:'Started streaming queries:' in text,timeout=900,interval=5,guard=self.guard)
        self.capture('deployed')
        self.save('stack_state.json',{'scope':self.scope,'namespace_uid':self.namespace_uid,'agent_url':self.agent_url,
            'source_root':str(self.source),'proxy_url':self.proxy,'source_version':self.scope.get('source_version')})

    def rpc(self,op,args=None,timeout=60,read_retries=3):
        self.guard()
        request=urllib.request.Request(self.agent_url+'/invoke',method='POST',
            data=json.dumps({'op':op,'args':args or {}},ensure_ascii=False).encode(),headers={'Content-Type':'application/json'})
        # Only observations can be retried. Never replay publish/crawl/listen RPCs:
        # a timed-out producer may already have committed its event to the broker.
        read_only=op in ('hdfs_records','hdfs','checkpoints','observe','end_offsets','versions')
        if op=='http':
            value=args or {};path=urllib.parse.urlparse(value.get('url','')).path
            method=value.get('method','GET')
            read_only=method=='GET' or (method=='POST' and path.endswith(('/_search','/_mget')))
        attempts=min(3,max(1,read_retries)) if read_only else 1
        for attempt in range(attempts):
            try:
                with urllib.request.urlopen(request,timeout=timeout) as response:data=json.load(response)
                return data['result']
            except urllib.error.HTTPError as exc:
                detail=sanitize(exc.read().decode())[:1200]
                try:transient=json.loads(detail).get('error') in ('TimeoutError','URLError','ConnectionError')
                except (ValueError,AttributeError):transient=False
                if transient and read_only and attempt+1==attempts:
                    raise ObservationUnavailable('Observer '+op+' temporarily unavailable: '+detail) from exc
                if not transient or attempt+1==attempts:
                    raise RuntimeError('Observer '+op+' failed: '+detail) from exc
            except (urllib.error.URLError,TimeoutError) as exc:
                detail=sanitize(exc)
                if attempt+1==attempts:
                    error=ObservationUnavailable if read_only else RuntimeError
                    raise error('Observer '+op+' failed: '+detail) from exc
            self.trace('observer_retry','RETRY',{'op':op,'attempt':attempt+1,'reason':detail})
            self.guard();threading.Event().wait(.5*(attempt+1))

    def http(self,path,method='GET',body=None,service='music-api',timeout=60):
        port=8000 if service=='music-api' else 9200
        return self.rpc('http',{'method':method,'url':f'http://{service}:{port}'+path,'body':body},timeout=timeout)

    def case_id(self,name):self.counter+=1;return self.run_id+'-'+name+'-'+str(self.counter)

    def publish(self,messages):
        self.trace('input','OBSERVED',messages)
        acks=self.rpc('publish',{'messages':messages},timeout=120)
        self.trace('kafka_ack','PASS',acks)
        observed=self.rpc('observe',{'positions':acks},timeout=90)
        assert {(r['topic'],r['partition'],r['offset'],r['value']) for r in observed}=={
            (r['topic'],r['partition'],r['offset'],r['value']) for r in acks},'Kafka bytes/positions differ'
        self.trace('kafka_observe','PASS',observed)
        return acks

    def await_records(self,acks,timeout=150):
        started=time.monotonic();needed={(r['topic'],r['partition'],r['offset']) for r in acks}
        # These three topics feed the raw insight index; events/comments/trends
        # still require HDFS reconciliation and their own typed-sink assertions.
        insight_acks=[a for a in acks if a['topic'] in ('music.charts','music.playlists','music.country-charts')]
        def observe_hdfs():
            remaining=started+timeout-time.monotonic()
            if remaining<=0:return None
            try:return self.rpc('hdfs_records',timeout=min(30,remaining),read_retries=1)
            except ObservationUnavailable as exc:
                self.trace('observer_eventual_retry','RETRY',{'op':'hdfs_records','reason':str(exc)})
                return None
        hdfs=poll(observe_hdfs,
            lambda data:data is not None and needed<={(r['topic'],r['partition'],r['offset']) for r in data['rows']},timeout=timeout,interval=3,guard=self.guard)
        actual={(r['topic'],r['partition'],r['offset']):r for r in hdfs['rows']}
        for a in acks:assert actual[(a['topic'],a['partition'],a['offset'])]['value']==a['value']
        self.trace('hdfs','PASS',{'rows':[actual[k] for k in sorted(needed)],'wait_seconds':time.monotonic()-started})
        es=None
        if insight_acks:
            def fetch():return self.http('/music-insight-raw/_mget',method='POST',body={'ids':[a['doc_id'] for a in insight_acks]},service='elasticsearch')
            es=poll(fetch,lambda r:r['status']==200 and all(d.get('found') for d in r['body'].get('docs',[])),timeout=timeout,interval=3,guard=self.guard)
            assert len(es['body']['docs'])==len(insight_acks)
            for a,d in zip(insight_acks,es['body']['docs']):assert d['_source']['value']==a['value']
            self.trace('elasticsearch','PASS',{'docs':es['body']['docs'],'wait_seconds':time.monotonic()-started})
        def covered(checkpoints):
            for name,expected in [('ingest',acks)]+([('insight-raw',insight_acks)] if insight_acks else []):
                if name not in checkpoints:return False
                source_offsets={}
                for record in checkpoints[name]['offset_log']:
                    if isinstance(record,dict):source_offsets.update({k:v for k,v in record.items() if k.startswith('music.')})
                if any(source_offsets.get(a['topic'],{}).get(str(a['partition']),-1)<=a['offset'] for a in expected):return False
            return True
        checkpoints=poll(lambda:self.rpc('checkpoints'),covered,timeout=timeout,interval=3,guard=self.guard)
        self.trace('spark_checkpoints','PASS',checkpoints)
        self.trace('e2e_latency','MEASURED',{'ack_observed_to_hdfs_and_es_seconds':time.monotonic()-started,
            'poll_interval_seconds':3,'definition':'post-ack observer wait, not exact Spark processing time'})
        return {'hdfs':hdfs,'es':es,'checkpoints':checkpoints}

    def restart(self,app):
        self.guard(force=True);pod=self.pod(app);before={'uid':pod['metadata']['uid'],'name':pod['metadata']['name']}
        original_uids={p['metadata']['uid'] for p in self.list('Pod') if p['metadata'].get('labels',{}).get('app')==app}
        self.trace('fault_before','OBSERVED',{'app':app,**before})
        start=time.monotonic();self.delete('Pod',pod['metadata']['name'])
        def replacement():
            values=[p for p in self.list('Pod') if p['metadata'].get('labels',{}).get('app')==app
                    and p['metadata']['uid'] not in original_uids and not p['metadata'].get('deletionTimestamp')]
            for value in values:
                owned(value,self.run_id,self.namespace)
                if value.get('status',{}).get('containerStatuses') and all(c.get('ready') for c in value['status']['containerStatuses']):
                    return value
            return None
        after=poll(replacement,lambda p:p is not None and all(c.get('ready') for c in p['status'].get('containerStatuses',[]))
             and bool(p['status'].get('containerStatuses')),timeout=360,interval=3,guard=self.guard)
        seconds=time.monotonic()-start
        self.trace('recovery','MEASURED',{'app':app,'new_uid':after['metadata']['uid'],'pod_ready_seconds':seconds,
                                       'definition':'pod readiness; data/API recovery checked separately'})
        if app=='qa-agent':raise SafetyError('Observer restart requires a new owned port-forward')
        return seconds

    def es_fault(self,mode):
        self.guard(force=True)
        if mode not in ('','unavailable','latency'):raise SafetyError('Unknown bounded fault')
        code='from pathlib import Path; p=Path("/tmp/qa-es-fault"); p.write_text('+repr(mode)+')'
        self.exec('es-gate',['python','-c',code])
        self.trace('es_fault','INJECTED' if mode else 'RESTORED',{'mode':mode})

    def batch(self,name='batch_views.py',timeout=600):
        self.guard(force=True)
        if name not in ('batch_views.py','batch_similarity.py'):raise SafetyError('Unknown test batch entrypoint')
        job_name='qa-'+name.split('.')[0].replace('_','-')+'-'+str(int(time.time()))
        pod=resources(self.scope,self.source)
        driver=next(r for r in pod if r['kind']=='Deployment' and r['metadata']['name']=='speed-layer')
        template=driver['spec']['template'];template['metadata']['labels']['app']=job_name
        spec=template['spec'];spec['restartPolicy']='Never';c=spec['containers'][0]
        c['name']='batch';c['args']=['exec bash /opt/app/submit.sh '+name+' qa-batch 1'];c.pop('readinessProbe',None)
        obj={'apiVersion':'batch/v1','kind':'Job','metadata':{'name':job_name,'namespace':self.namespace,'labels':{LABEL:self.run_id}},
             'spec':{'backoffLimit':0,'activeDeadlineSeconds':timeout,'template':template}}
        self.create(obj)
        def finished():
            job=self.get('Job',job_name)
            return job if job.get('status',{}).get('succeeded') or job.get('status',{}).get('failed') else None
        try:job=poll(finished,timeout=timeout+30,interval=4,guard=self.guard)
        finally:
            try:self.save('batch/'+job_name+'.log',{'log':self.kube('logs','-n',self.namespace,'job/'+job_name,'--tail=250',timeout=30)})
            except Exception as exc:self.trace('batch_logs','INCONCLUSIVE',{'error':sanitize(exc)})
        self.trace('batch_job','PASS' if job['status'].get('succeeded') else 'FAIL',{'name':job_name,'status':job['status']})
        assert job['status'].get('succeeded')==1,'Real production batch job failed: '+job_name
        return job_name

    def capture(self,tag):
        pods=self.list('Pod') if self.namespace_uid else []
        inventory=[]
        for pod in pods:
            owned(pod,self.run_id,self.namespace)
            inventory.append({'name':pod['metadata']['name'],'uid':pod['metadata']['uid'],'node':pod['spec'].get('nodeName'),
                'phase':pod['status'].get('phase'),'containers':[{'name':c['name'],'image':c['image'],'resources':c.get('resources')} for c in pod['spec']['containers']],
                'statuses':pod['status'].get('containerStatuses',[])})
        self.save(tag+'/pods.json',inventory)
        self.save(tag+'/controllers.json',[{'kind':kind,'name':o['metadata']['name'],'uid':o['metadata']['uid'],
            'generation':o['metadata'].get('generation'),'revision':o['metadata'].get('annotations',{}).get('deployment.kubernetes.io/revision'),
            'status':o.get('status')} for kind in ('Deployment','StatefulSet','Job') for o in self.list(kind)])
        for app in ['speed-layer','spark-worker','music-api','kafka','hdfs-namenode','hdfs-datanode','elasticsearch']:
            try:
                text=self.kube('logs','-n',self.namespace,'-l','app='+app,'--all-containers=true','--prefix=true','--tail=200',timeout=40)
                self.save(tag+'/'+app+'.log',{'log':sanitize(text)})
            except Exception as exc:self.trace('capture_logs','INCONCLUSIVE',{'app':app,'error':sanitize(exc)})
        try:self.save(tag+'/events.json',self.request('GET',f'/api/v1/namespaces/{self.namespace}/events'))
        except Exception:pass

    def cleanup(self):
        outcomes=[]
        try:
            if self.namespace_uid:
                self.capture('before_cleanup')
                # Only journaled controllers/config, with UID preconditions. No PVC is created/deleted.
                for record in reversed(self.journal):
                    if record['kind']=='Namespace':continue
                    try:
                        obj=self.get(record['kind'],record['name'])
                        if obj['metadata']['uid']!=record['uid']:raise SafetyError('UID changed; refusing cleanup')
                        self.delete(record['kind'],record['name'])
                        outcomes.append(dict(record,result='DELETE_REQUESTED'))
                    except urllib.error.HTTPError as exc:
                        outcomes.append(dict(record,result='ALREADY_GONE' if exc.code==404 else 'NOT_DELETED',reason=str(exc)))
                    except Exception as exc:outcomes.append(dict(record,result='NOT_DELETED',reason=sanitize(exc)))
                # Namespace deletion is conditional on a full namespaced metadata inventory.
                def remaining():
                    return [p for p in self.list('Pod') if not p['metadata'].get('deletionTimestamp')]
                try:poll(remaining,lambda p:not p,timeout=120,interval=3)
                except TimeoutError:pass
                leftover=self.namespace_inventory()
                self.save('cleanup_remaining_metadata.json',leftover)
                allowed={('ServiceAccount','default'),('ConfigMap','kube-root-ca.crt')}
                journal_uids={r['uid'] for r in self.journal}
                foreign=[r for r in leftover if (r['kind'],r['name']) not in allowed and r['kind']!='Event'
                         and (r.get('labels') or {}).get(LABEL)!=self.run_id
                         and not any(o.get('uid') in journal_uids for o in r.get('owners',[]))]
                if not foreign:
                    self.delete('Namespace',self.namespace)
                    outcomes.append({'kind':'Namespace','name':self.namespace,'result':'DELETE_REQUESTED_AFTER_INVENTORY'})
                else:outcomes.append({'kind':'Namespace','name':self.namespace,'result':'RETAINED','reason':'Remaining/unknown resources','resources':foreign})
        finally:
            if self.proxy:
                try:self.save('baseline_after.json',self._baseline())
                except Exception as exc:outcomes.append({'result':'AFTER_SNAPSHOT_FAILED','reason':sanitize(exc)})
            self.save('cleanup.json',outcomes)
            for p,handle in reversed(self.processes):
                if p.poll() is None:
                    p.terminate()
                    try:p.wait(timeout=10)
                    except subprocess.TimeoutExpired:p.kill();p.wait(timeout=5)
                handle.close()

    def namespace_inventory(self):
        targets=[]
        for prefix in ['/api/v1']:
            for r in self.request('GET',prefix)['resources']:
                if r.get('namespaced') and 'list' in r.get('verbs',[]) and '/' not in r['name']:
                    targets.append((prefix,r))
        for group in self.request('GET','/apis')['groups']:
            # Metrics are cached observations, not namespace-owned stored objects;
            # PodMetrics may outlive their Pod briefly and have no metadata UID.
            if group['name'] in ('metrics.k8s.io','custom.metrics.k8s.io','external.metrics.k8s.io'):
                continue
            prefix='/apis/'+group['preferredVersion']['groupVersion']
            for r in self.request('GET',prefix).get('resources',[]):
                if r.get('namespaced') and 'list' in r.get('verbs',[]) and '/' not in r['name']:targets.append((prefix,r))
        if len(targets)>200:raise SafetyError('Cleanup discovery budget exceeded; namespace retained')
        def fetch(target):
            prefix,r=target;path=prefix+'/namespaces/'+self.namespace+'/'+r['name']
            request=urllib.request.Request(self.proxy+path,headers={'Accept':'application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1'})
            with urllib.request.urlopen(request,timeout=15) as response:data=json.load(response)
            return [{'kind':r['kind'],'name':x['metadata']['name'],'uid':x['metadata'].get('uid'),
                     'labels':x['metadata'].get('labels'),'owners':x['metadata'].get('ownerReferences',[]),
                     'deleting':bool(x['metadata'].get('deletionTimestamp'))} for x in (data.get('items') or [])]
        result=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            for values in pool.map(fetch,targets):result.extend(values)
        return result
