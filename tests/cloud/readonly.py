"""Bounded observation of an explicitly selected live namespace. No writes/faults."""
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.request

from .harness import Harness,process
from .safety import SafetyError,poll,sha256

MAX_RESPONSE_BYTES=4*1024*1024  # finite observer budget, not an API response-size contract


class ReadOnly(Harness):
    def __init__(self,scope,source,report,namespace):
        super().__init__(scope,source,report)
        if namespace!='music':raise SafetyError('This approved read-only campaign is scoped to music')
        self.live_namespace=namespace;self.urls={}

    def observe_open(self):
        cluster=json.loads(process(['gcloud.cmd','container','clusters','describe',self.scope['cluster'],
           '--zone='+self.scope['location'],'--project='+self.scope['project'],'--format=json(name,location,endpoint)']))
        server=self.kube('config','view','--minify','-o','jsonpath={.clusters[0].cluster.server}').strip()
        if server!='https://'+cluster['endpoint']:raise SafetyError('Read-only cluster identity mismatch')
        pods=json.loads(self.kube('get','pods','-n',self.live_namespace,'-o','json'))['items']
        cms=json.loads(self.kube('get','configmaps','-n',self.live_namespace,'-o','json'))['items']
        self.pods=pods;self.configmaps={c['metadata']['name']:c for c in cms}
        self.env=self.configmaps['music-env']['data']
        self.save('live_identity.json',{'cluster':cluster,'context':self.scope['context'],'namespace':self.live_namespace,
            'pods':[{'name':p['metadata']['name'],'uid':p['metadata']['uid'],'node':p['spec'].get('nodeName'),
                'status':p['status'],'resources':[c.get('resources') for c in p['spec']['containers']]} for p in pods],
            'configmap_hashes':{name:{k:sha256(v.encode()) for k,v in c.get('data',{}).items()} for name,c in self.configmaps.items()}})
        self.api_pod=next(p['metadata']['name'] for p in pods if p['metadata'].get('labels',{}).get('app')=='music-api' and p['status'].get('phase')=='Running')
        for service,remote in [('music-api',80),('elasticsearch',9200)]:
            p,log=self._spawn(['kubectl','--context='+self.scope['context'],'-n',self.live_namespace,
               'port-forward','service/'+service,':'+str(remote),'--address=127.0.0.1'],'readonly-'+service)
            def address():
                text=log.read_text(encoding='utf-8',errors='replace');match=re.search(r'127\.0\.0\.1:(\d+)',text)
                if p.poll() is not None:raise RuntimeError(text[-500:])
                return 'http://127.0.0.1:'+match.group(1) if match else None
            self.urls[service]=poll(address,timeout=30,interval=.2)

    def read(self,path,service='music-api',body=None):
        # ES _search uses POST with a read-only DSL. Every other path is GET.
        if body is not None and not path.endswith('/_search'):raise SafetyError('Read-only search endpoint required')
        request=urllib.request.Request(self.urls[service]+path,data=json.dumps(body).encode() if body is not None else None,
            headers={'Content-Type':'application/json'})
        start=time.perf_counter()
        try:
            with urllib.request.urlopen(request,timeout=25) as response:status=response.status;raw=response.read(MAX_RESPONSE_BYTES+1)
        except urllib.error.HTTPError as exc:status=exc.code;raw=exc.read(MAX_RESPONSE_BYTES+1)
        if len(raw)>MAX_RESPONSE_BYTES:raise SafetyError('Read-only response budget exceeded')
        try:value=json.loads(raw)
        except ValueError:value=raw.decode('utf-8',errors='replace')[:500]
        result={'status':status,'body':value,'body_bytes':len(raw),'latency_ms':(time.perf_counter()-start)*1000}
        self.trace('live_http','OBSERVED',{'service':service,'path':path,**result})
        return result

    def kafka_record(self,topic,partition,offset):
        if topic not in ('music.events','music.charts','music.playlists','music.country-charts'):raise SafetyError('Unapproved read-only topic')
        code='''import json,time
from kafka import KafkaConsumer,TopicPartition
c=KafkaConsumer(bootstrap_servers=BOOTSTRAP,enable_auto_commit=False,group_id=None,request_timeout_ms=10000)
try:
 t=TopicPartition(TOPIC,PARTITION);c.assign([t]);c.seek(t,OFFSET);end=time.monotonic()+12;found=None
 while time.monotonic()<end and found is None:
  for rows in c.poll(timeout_ms=300,max_records=50).values():
   for r in rows:
    if r.offset==OFFSET:found={'topic':r.topic,'partition':r.partition,'offset':r.offset,'value':r.value.decode()}
 print(json.dumps(found,ensure_ascii=False))
finally:c.close()
'''
        prefix='BOOTSTRAP='+repr(self.env['KAFKA_BOOTSTRAP'])+';TOPIC='+repr(topic)+';PARTITION='+str(int(partition))+';OFFSET='+str(int(offset))+'\n'
        result=json.loads(self.kube('exec','-n',self.live_namespace,self.api_pod,'--','python','-c',prefix+code,timeout=40))
        self.trace('live_kafka_seek','OBSERVED',result);return result

    def close(self):
        for p,handle in reversed(self.processes):
            if p.poll() is None:
                p.terminate()
                try:p.wait(timeout=10)
                except subprocess.TimeoutExpired:p.kill();p.wait(timeout=5)
            handle.close()
        self.save('readonly_cleanup.json',{'port_forwards_closed':True,'resources_created':0,'data_written':0})
