"""Disposable same-code stack: no PVC/PD/LB, no production resource mutation."""
import copy
from pathlib import Path
import re

import yaml

from .safety import SafetyError,sha256,validate_scope

LABEL='qa.trend2playlist/run'
SPARK_IMAGE='apache/spark:3.5.9-scala2.12-java17-python3-ubuntu'


def resources(scope,source):
    validate_scope(scope);ns=scope['namespace'];run=scope['run_id'];labels={LABEL:run}
    source=Path(source);result=[]
    def add(obj):
        obj=copy.deepcopy(obj);obj.setdefault('metadata',{})['labels']={**obj.get('metadata',{}).get('labels',{}),**labels}
        if obj['kind']!='Namespace':obj['metadata']['namespace']=ns
        spec=obj.get('spec',{})
        if obj['kind'] in ('Deployment','StatefulSet','Job'):
            template=spec['template'];template['metadata'].setdefault('labels',{}).update(labels)
            pod=template['spec'];pod.update(serviceAccountName='qa-runner',automountServiceAccountToken=False,enableServiceLinks=False)
            for container in pod.get('containers',[]):
                container.setdefault('resources',{}).setdefault('requests',{}).setdefault('ephemeral-storage','32Mi')
                container['resources'].setdefault('limits',{}).setdefault('ephemeral-storage','1Gi')
        result.append(obj)
        return obj
    def cm(name,data):return add({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':name},'data':data})
    def service(name,ports,app=None,headless=False):
        spec={'selector':{'app':app or name},'ports':[{'name':str(p),'port':p,'targetPort':p} for p in ports]}
        if headless:spec.update(clusterIP='None',publishNotReadyAddresses=True)
        return add({'apiVersion':'v1','kind':'Service','metadata':{'name':name},'spec':spec})
    def deployment(name,image,args,memory='512Mi',request='256Mi',cpu='300m',env=None,volumes=None,mounts=None,command=None):
        container={'name':name,'image':image,'command':command or ['/bin/bash','-c'],'args':args,
            'resources':{'requests':{'cpu':'100m','memory':request},'limits':{'cpu':cpu,'memory':memory}},
            'env':[{'name':k,'value':str(v)} for k,v in (env or {}).items()],
            'volumeMounts':mounts or []}
        return add({'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':name},'spec':{
            'replicas':1,'strategy':{'type':'Recreate'},'selector':{'matchLabels':{'app':name}},
            'template':{'metadata':{'labels':{'app':name}},'spec':{'containers':[container],'volumes':volumes or []}}}})

    add({'apiVersion':'v1','kind':'Namespace','metadata':{'name':ns}})
    add({'apiVersion':'v1','kind':'ResourceQuota','metadata':{'name':'qa-budget'},'spec':{'hard':{
        'requests.cpu':'3','limits.cpu':'8','requests.memory':'7Gi','limits.memory':'14Gi','pods':'18',
        'persistentvolumeclaims':'0','services.loadbalancers':'0','services.nodeports':'0',
        'count/jobs.batch':'6','count/horizontalpodautoscalers.autoscaling':'0'}}})
    add({'apiVersion':'v1','kind':'LimitRange','metadata':{'name':'qa-limits'},'spec':{'limits':[{
        'type':'Container','default':{'cpu':'300m','memory':'512Mi'},'defaultRequest':{'cpu':'50m','memory':'128Mi'}}]}})
    add({'apiVersion':'v1','kind':'ServiceAccount','metadata':{'name':'qa-runner'},'automountServiceAccountToken':False})
    add({'apiVersion':'networking.k8s.io/v1','kind':'NetworkPolicy','metadata':{'name':'qa-isolation'},'spec':{
        'podSelector':{},'policyTypes':['Ingress','Egress'],
        'ingress':[{'from':[{'namespaceSelector':{'matchLabels':{'kubernetes.io/metadata.name':ns}}}]}],
        'egress':[{'to':[{'namespaceSelector':{'matchLabels':{'kubernetes.io/metadata.name':ns}}}]},
          {'to':[{'namespaceSelector':{'matchLabels':{'kubernetes.io/metadata.name':'kube-system'}}}],
           'ports':[{'protocol':'UDP','port':53},{'protocol':'TCP','port':53}]},
          {'to':[{'ipBlock':{'cidr':'169.254.20.10/32'}}],'ports':[{'protocol':'UDP','port':53},{'protocol':'TCP','port':53}]},
          {'to':[{'ipBlock':{'cidr':'0.0.0.0/0','except':['10.0.0.0/8','172.16.0.0/12','192.168.0.0/16','169.254.0.0/16']}}],
           'ports':[{'protocol':'TCP','port':80},{'protocol':'TCP','port':443}]}]}})

    env={'KAFKA_BOOTSTRAP':','.join(f'kafka-{i}.kafka-headless.{ns}.svc.cluster.local:9092' for i in range(3)),
         'ES_URL':'http://es-gate:9200','ES_NODES':'es-gate','ES_PORT':'9200',
         'DATA_BASE':f'hdfs://hdfs-namenode.{ns}.svc.cluster.local:8020/qa/{run}',
         'SPARK_MASTER':f'spark://spark-master.{ns}.svc.cluster.local:7077',
         'SPARK_PACKAGES':'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.elasticsearch:elasticsearch-spark-30_2.12:8.19.13',
         'EXECUTOR_MEMORY':'512m','HDFS_REPLICATION':'2','STATE_STORE':'rocksdb','TZ_NAME':'Asia/Ho_Chi_Minh',
         'TZ':'Asia/Ho_Chi_Minh','PYTHONUNBUFFERED':'1','SPEED_CORES_MAX':'1','BATCH_CORES_MAX':'1',
         'SHUFFLE_PARTITIONS':'3','INGEST_TRIGGER':'5 seconds','PLAYS_TRIGGER':'3 seconds',
         'CHARTS_TRIGGER':'5 seconds','MENTIONS_TRIGGER':'5 seconds','RECO_TRIGGER':'5 seconds',
         'PROGRESS_EVERY_SEC':'5','VIEW_TTL_SEC':'10','INSIGHT_BACKEND':'elasticsearch',
         'QA_NAMESPACE':ns,'QA_RUN_ID':run}
    cm('music-env',env)
    spark_files={p.name:p.read_text(encoding='utf-8') for p in (source/'spark').glob('*.py')}
    spark_files['submit.sh']=(source/'spark/submit.sh').read_text(encoding='utf-8').replace('\r\n','\n')
    spark_files.update({p.name:p.read_text(encoding='utf-8') for p in (source/'shared').glob('*.py')})
    cm('spark-code',spark_files)
    serving_files={p.name:p.read_text(encoding='utf-8') for p in (source/'serving').glob('*.py')}
    serving_files['requirements.txt']=(source/'serving/requirements.txt').read_text(encoding='utf-8')
    serving_files.update({p.name:p.read_text(encoding='utf-8') for p in (source/'shared').glob('*.py')})
    cm('serving-code',serving_files)
    cm('dashboard-static',{p.name:p.read_text(encoding='utf-8') for p in (source/'serving/static').glob('*') if p.is_file()})
    crawler_files={p.name:p.read_text(encoding='utf-8') for p in (source/'crawler').glob('*.py')}
    crawler_files.update({p.name:p.read_text(encoding='utf-8') for p in (source/'shared').glob('*.py')})
    crawler_files['agent.py']=(source/'tests/cloud/agent.py').read_text(encoding='utf-8')
    crawler_files['requirements.txt']='requests>=2.31,<3\nkafka-python>=2.0.2,<3\npyarrow==17.0.0\n'
    cm('qa-agent-code',crawler_files)
    cm('qa-gate-code',{'es_gate.py':(source/'tests/cloud/es_gate.py').read_text(encoding='utf-8')})

    # Reuse production Kafka/HDFS definitions with explicit resource/profile overrides.
    for filename in ['10-kafka.yaml','20-hdfs.yaml']:
        text=(source/'k8s'/filename).read_text(encoding='utf-8').replace('.music.svc.cluster.local','.'+ns+'.svc.cluster.local')
        for obj in yaml.safe_load_all(text):
            if not obj or obj['kind']=='PodDisruptionBudget':continue
            if obj['kind']=='ConfigMap':
                add(obj);continue
            if obj['kind']=='StatefulSet':
                obj['spec'].pop('volumeClaimTemplates',None)
                pod=obj['spec']['template']['spec'];pod.setdefault('volumes',[]).append({'name':'data','emptyDir':{'sizeLimit':'1Gi'}})
                for c in pod['containers']:
                    c['resources']={'requests':{'cpu':'100m','memory':'256Mi'},'limits':{'cpu':'300m','memory':'640Mi'}}
                for init in pod.get('initContainers',[]):
                    init['resources']={'requests':{'cpu':'25m','memory':'32Mi'},'limits':{'cpu':'100m','memory':'64Mi'}}
            add(obj)

    service('elasticsearch',[9200])
    es=deployment('elasticsearch','docker.elastic.co/elasticsearch/elasticsearch:8.19.22',[],
        memory='1Gi',request='512Mi',cpu='500m',command=['/bin/tini','--','/usr/local/bin/docker-entrypoint.sh'],
        env={'discovery.type':'single-node','xpack.security.enabled':'false','ES_JAVA_OPTS':'-Xms256m -Xmx256m'},
        volumes=[{'name':'data','emptyDir':{'sizeLimit':'1Gi'}}],mounts=[{'name':'data','mountPath':'/usr/share/elasticsearch/data'}])
    es['spec']['template']['spec']['securityContext']={'fsGroup':1000}
    es['spec']['template']['spec']['containers'][0]['args']=['eswrapper']
    es['spec']['template']['spec']['containers'][0]['readinessProbe']={'httpGet':{'path':'/','port':9200},'initialDelaySeconds':15,'periodSeconds':5}
    service('es-gate',[9200])
    gate=deployment('es-gate','python:3.12-slim',['python /qa/es_gate.py'],memory='192Mi',request='64Mi',cpu='150m',
        volumes=[{'name':'code','configMap':{'name':'qa-gate-code'}}],mounts=[{'name':'code','mountPath':'/qa'}])
    gate['spec']['template']['spec']['containers'][0]['readinessProbe']={'tcpSocket':{'port':9200},'periodSeconds':3}

    service('spark-master',[7077,8080])
    master=deployment('spark-master',SPARK_IMAGE,['exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host "$POD_IP" --port 7077 --webui-port 8080'],
        memory='384Mi',request='128Mi',cpu='200m',env={'SPARK_DAEMON_MEMORY':'128m','SPARK_MASTER_HOST':'spark-master'})
    master['spec']['template']['spec']['containers'][0]['readinessProbe']={'tcpSocket':{'port':7077},'periodSeconds':5}
    master['spec']['template']['spec']['containers'][0]['env'].append({'name':'POD_IP','valueFrom':{'fieldRef':{'fieldPath':'status.podIP'}}})
    worker=deployment('spark-worker',SPARK_IMAGE,['exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker --host "$POD_IP" --cores 2 --memory 1400m spark://spark-master:7077'],
        memory='2304Mi',request='1536Mi',cpu='1200m',env={'SPARK_DAEMON_MEMORY':'128m'})
    worker['spec']['template']['spec']['containers'][0]['env'].append({'name':'POD_IP','valueFrom':{'fieldRef':{'fieldPath':'status.podIP'}}})
    worker['spec']['template']['spec']['containers'][0]['readinessProbe']={'tcpSocket':{'port':8081},'periodSeconds':5,'initialDelaySeconds':10}
    driver=deployment('speed-layer',SPARK_IMAGE,['exec bash /opt/app/submit.sh speed_layer.py cloud-e2e-speed 1'],
        memory='1536Mi',request='896Mi',cpu='750m',env={'DRIVER_MEMORY':'700m','SPEED_QUERIES':'ingest,plays,mentions,charts,recommend'},
        volumes=[{'name':'code','configMap':{'name':'spark-code'}}],mounts=[{'name':'code','mountPath':'/opt/app'}])
    c=driver['spec']['template']['spec']['containers'][0];c['envFrom']=[{'configMapRef':{'name':'music-env'}}]
    c['env'].append({'name':'POD_IP','valueFrom':{'fieldRef':{'fieldPath':'status.podIP'}}})
    # Start driver after dependency rollout; UI readiness is not streaming readiness.
    c['readinessProbe']={'tcpSocket':{'port':4040},'initialDelaySeconds':30,'periodSeconds':5}
    service('speed-layer-ui',[4040],app='speed-layer')
    api=deployment('music-api','python:3.12-slim',['pip install -q --no-cache-dir -r /app/requirements.txt && exec uvicorn api:app --host 0.0.0.0 --port 8000'],
        memory='384Mi',request='128Mi',cpu='350m',volumes=[{'name':'code','configMap':{'name':'serving-code'}}],mounts=[{'name':'code','mountPath':'/app'}])
    api_c=api['spec']['template']['spec']['containers'][0];api_c['workingDir']='/app';api_c['envFrom']=[{'configMapRef':{'name':'music-env'}}]
    api_c['env'].append({'name':'STATIC_DIR','value':'/static'})
    api['spec']['template']['spec']['volumes'].append({'name':'static','configMap':{'name':'dashboard-static'}})
    api_c['volumeMounts'].append({'name':'static','mountPath':'/static'})
    api_c['readinessProbe']={'httpGet':{'path':'/api/health','port':8000},'initialDelaySeconds':15,'periodSeconds':5}
    api_c['livenessProbe']={'httpGet':{'path':'/api/live','port':8000},'initialDelaySeconds':45,'periodSeconds':10}
    service('music-api',[8000])
    agent=deployment('qa-agent','python:3.12-slim',['pip install -q --no-cache-dir -r /app/requirements.txt && exec python /app/agent.py'],
        memory='512Mi',request='128Mi',cpu='400m',volumes=[{'name':'code','configMap':{'name':'qa-agent-code'}}],mounts=[{'name':'code','mountPath':'/app'}])
    agent_c=agent['spec']['template']['spec']['containers'][0];agent_c['workingDir']='/app';agent_c['envFrom']=[{'configMapRef':{'name':'music-env'}}]
    agent_c['readinessProbe']={'httpGet':{'path':'/','port':8899},'initialDelaySeconds':10,'periodSeconds':5}
    service('qa-agent',[8899])

    for obj in result:
        if obj['kind']=='Service' and obj['spec'].get('type','ClusterIP')!='ClusterIP':raise SafetyError('External service forbidden')
        if obj['kind']=='PersistentVolumeClaim' or obj.get('spec',{}).get('volumeClaimTemplates'):raise SafetyError('New persistent disk forbidden')
        if obj['kind']=='ConfigMap' and len(str(obj).encode())>900000:raise SafetyError('ConfigMap budget exceeded')
    return result
