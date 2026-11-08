"""In-cluster observer/fixture server. Real HTTP/Kafka/HDFS, no arbitrary exec RPC."""
import concurrent.futures
import datetime
import io
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer

from kafka import KafkaProducer,KafkaConsumer,TopicPartition
from kafka.admin import KafkaAdminClient,NewTopic

NS=os.environ['QA_NAMESPACE'];RUN=os.environ['QA_RUN_ID']
BOOTSTRAP=os.environ['KAFKA_BOOTSTRAP'];BASE='/qa/'+RUN
ALLOWED={'music-api','elasticsearch','es-gate','spark-master','speed-layer-ui','hdfs-namenode'}
TOPICS=['music.charts','music.playlists','music.country-charts','music.events','music.comments','music.trends']
FIXTURE={'feed':{'results':[]}}
SLOTS=threading.BoundedSemaphore(4)
PARQUET_CACHE={}
HDFS_CACHE={};HDFS_CACHE_BYTES=0;BAD_DATANODES={};HDFS_LOCK=threading.Lock()


def http(method,url,body=None,timeout=20):
    parsed=urllib.parse.urlparse(url)
    if parsed.scheme!='http' or parsed.hostname not in ALLOWED:raise ValueError('Only owned in-namespace services allowed')
    request=urllib.request.Request(url,method=method,data=json.dumps(body).encode('utf-8') if body is not None else None,
        headers={'Content-Type':'application/json'})
    start=time.perf_counter()
    try:
        with urllib.request.urlopen(request,timeout=timeout) as response:
            content=response.read();status=response.status
    except urllib.error.HTTPError as exc:content=exc.read();status=exc.code
    text=content.decode('utf-8',errors='replace')
    try:data=json.loads(text)
    except ValueError:data=text[:2000]
    return {'status':status,'body':data,'latency_ms':(time.perf_counter()-start)*1000,'observed_ms':int(time.time()*1000)}


def hdfs(path,op='LISTSTATUS'):
    global HDFS_CACHE_BYTES
    if not path.startswith(BASE+'/') or '..' in path.split('/'):raise ValueError('HDFS path outside this test run')
    url='http://hdfs-namenode:9870/webhdfs/v1'+urllib.parse.quote(path,safe='/')+'?op='+op
    if op=='OPEN':
        import requests
        import ipaddress
        status=hdfs(path,'GETFILESTATUS')['FileStatus']
        version=(status['modificationTime'],status['length'])
        with HDFS_LOCK:
            cached=HDFS_CACHE.get(path)
            if cached and cached[0]==version:return cached[1]
        last=None
        for attempt in range(3):
            with HDFS_LOCK:
                excluded=[host for host,until in BAD_DATANODES.items() if until>time.monotonic()]
            target=url+('&excludedatanodes='+urllib.parse.quote(','.join(excluded),safe='') if excluded else '')
            response=requests.get(target,allow_redirects=False,timeout=(3,10))
            if response.status_code>=400:
                try:remote=json.loads(response.content).get('RemoteException',{})
                except (ValueError,AttributeError):remote={}
                if (excluded and response.status_code==403 and remote.get('exception')=='IOException'
                        and remote.get('message')=='No active nodes contain this block'):
                    # This is per-request replica selection exhaustion, not an
                    # authorization denial. Reprobe within the caller deadline.
                    with HDFS_LOCK:BAD_DATANODES.clear()
                    raise TimeoutError('Temporary WebHDFS replica exclusions exhausted; reset for bounded reprobe')
                if excluded and response.status_code>=500 and attempt<2:
                    with HDFS_LOCK:BAD_DATANODES.clear()
                    continue
                raise urllib.error.HTTPError(target,response.status_code,'WebHDFS read failed',{},io.BytesIO(response.content))
            location=response.headers.get('Location') if response.status_code in (302,307) else None
            if location:
                parsed=urllib.parse.urlparse(location);host=parsed.hostname or ''
                try:private_ip=ipaddress.ip_address(host).is_private
                except ValueError:private_ip=False
                if parsed.scheme!='http' or parsed.port!=9864 or not (host.endswith('.'+NS+'.svc.cluster.local') or private_ip):
                    raise ValueError('Unexpected DataNode redirect outside owned HDFS')
                try:
                    data=requests.get(location,timeout=(3,10))
                    if data.status_code>=400:raise OSError('DataNode read status '+str(data.status_code))
                    value=data.content
                except (requests.exceptions.RequestException,OSError) as exc:
                    last=exc
                    # Match Hadoop's WebHDFS client: request a different replica
                    # instead of following the same stale DataNode redirect again.
                    # NameNode excludes by registered transfer IP/port. Redirect
                    # DNS/HTTP endpoints are not equivalent, especially after
                    # a StatefulSet pod receives a new IP during replacement.
                    blocks=hdfs(path,'GET_BLOCK_LOCATIONS')['LocatedBlocks']['locatedBlocks']
                    addresses={str(node['ipAddr'])+':'+str(node['xferPort'])
                        for block in blocks for node in block['locations']
                        if host.rstrip('.').lower() in (node['hostName'].rstrip('.').lower(),node['ipAddr'])}
                    if not addresses:raise ConnectionError('Failed DataNode absent from current block locations') from exc
                    with HDFS_LOCK:
                        for address in addresses:BAD_DATANODES[address]=time.monotonic()+20
                    continue
            else:value=response.content
            # Revalidate NameNode file version: caching must not hide a regenerated
            # query ID or a changed/corrupted checkpoint during recovery testing.
            with HDFS_LOCK:
                previous=HDFS_CACHE.get(path)
                previous_size=len(previous[1]) if previous else 0
                if len(value)<128*1024 and HDFS_CACHE_BYTES-previous_size+len(value)<=8*1024*1024:
                    HDFS_CACHE[path]=(version,value);HDFS_CACHE_BYTES+=len(value)-previous_size
            return value
        raise TimeoutError('No readable HDFS replica after bounded failover: '+str(last))
    with urllib.request.urlopen(url,timeout=15) as response:
        value=response.read()
    return value if op=='OPEN' else json.loads(value)


def checkpoints():
    result={}
    for name in ['insight-raw','ingest','plays','mentions','charts','recommend']:
        root=BASE+'/checkpoints/'+('plays-v2' if name=='plays' else name)
        try:
            files=hdfs(root+'/commits')['FileStatuses']['FileStatus']
            ids=[int(f['pathSuffix']) for f in files if f['pathSuffix'].isdigit()]
            if not ids:continue
            batch=max(ids)
            lines=hdfs(root+'/offsets/'+str(batch),'OPEN').decode().splitlines()
            parsed=[]
            for line in lines[1:]:
                try:parsed.append(json.loads(line))
                except ValueError:pass
            meta=json.loads(hdfs(root+'/metadata','OPEN'))
            result[name]={'batch_id':batch,'query_id':meta.get('id'),'offset_log':parsed,'checkpoint_path':root}
        except urllib.error.HTTPError as exc:
            if exc.code!=404:raise
    return result


def committed_parquet_files():
    """Match Spark's FileStreamSinkLog, including compact snapshots and deletes."""
    import re
    root=BASE+'/datalake/raw';metadata=root+'/_spark_metadata'
    try:entries=hdfs(metadata)['FileStatuses']['FileStatus']
    except urllib.error.HTTPError as exc:
        if exc.code==404:return {'files':[],'logs':[]}
        raise
    logs={e['pathSuffix'] for e in entries if re.fullmatch(r'\d+(?:\.compact)?',e['pathSuffix'])}
    compact=max((int(name.split('.')[0]) for name in logs if name.endswith('.compact')),default=-1)
    selected=([str(compact)+'.compact'] if compact>=0 else [])+sorted(
        [name for name in logs if name.isdigit() and int(name)>compact],key=int)
    if len(selected)>1000:raise ValueError('Bounded HDFS commit log scan exceeded')
    files={}
    for name in selected:
        try:lines=hdfs(metadata+'/'+name,'OPEN').decode().splitlines()
        except urllib.error.HTTPError as exc:
            if exc.code==404:raise TimeoutError('Commit log compacted during observation') from exc
            raise
        if not lines or lines[0]!='v1':raise ValueError('Unsupported Spark file sink log version')
        for line in lines[1:]:
            record=json.loads(line);parsed=urllib.parse.urlparse(record['path'])
            path=urllib.parse.unquote(parsed.path)
            if parsed.scheme not in ('','hdfs') or not path.startswith(root+'/') or '..' in path.split('/'):
                raise ValueError('Committed path outside owned raw dataset')
            if record['action']=='delete':files.pop(path,None)
            elif record['action']=='add':
                if not record.get('isDir'):
                    if not path.endswith('.parquet'):raise ValueError('Non-Parquet committed raw file')
                    files[path]=dict(record,path=path)
            else:raise ValueError('Unsupported Spark file sink action')
    return {'files':[files[path] for path in sorted(files)],'logs':selected}


def hdfs_records():
    import pyarrow.parquet as pq
    committed=committed_parquet_files();files=committed['files'];total=sum(f['size'] for f in files);rows=[]
    if len(files)>500 or total>16*1024*1024:raise ValueError('Bounded HDFS scan exceeded')
    for entry in files:
        path=entry['path']
        if path not in PARQUET_CACHE:
            value=hdfs(path,'OPEN')
            if len(value)<8 or len(value)!=entry['size']:
                raise ValueError('Committed Parquet payload invalid: '+path+' expected='+str(entry['size'])+' actual='+str(len(value)))
            PARQUET_CACHE[path]=pq.read_table(io.BytesIO(value)).to_pylist()
        for source_row in PARQUET_CACHE[path]:
            row=dict(source_row)
            row['topic']=next(p[6:] for p in path.split('/') if p.startswith('topic='))
            row['dt']=next(p[3:] for p in path.split('/') if p.startswith('dt='))
            row['hdfs_path']=path
            stamp=row.get('kafka_ts')
            if stamp:
                if stamp.tzinfo is None:stamp=stamp.replace(tzinfo=datetime.timezone.utc)
                row['kafka_ts_ms']=int(stamp.timestamp()*1000);row.pop('kafka_ts',None)
            rows.append(row)
            if len(rows)>5000:raise ValueError('HDFS row budget exceeded')
    return {'rows':rows,'files':len(files),'bytes':total,'commit_logs':committed['logs']}


def producer():
    return KafkaProducer(bootstrap_servers=BOOTSTRAP.split(','),acks='all',retries=3,request_timeout_ms=10000,
        max_block_ms=15000,key_serializer=lambda x:str(x).encode('utf-8'))


def publish(messages):
    if len(messages)>100:raise ValueError('At most100 messages per RPC')
    p=producer();acks=[]
    try:
        for i,message in enumerate(messages):
            topic=message['topic']
            if topic not in TOPICS:raise ValueError('Unknown owned topic')
            raw=message.get('raw')
            if raw is None:raw=json.dumps(message['record'],ensure_ascii=False,allow_nan=False)
            if len(raw.encode())>65536:raise ValueError('Message size budget exceeded')
            start=time.monotonic()
            future=p.send(topic,key=message.get('key',RUN),value=raw.encode(),
                          **({'partition':message['partition']} if 'partition' in message else {}),
                          **({'timestamp_ms':message['kafka_timestamp_ms']} if 'kafka_timestamp_ms' in message else {}))
            meta=future.get(timeout=15)
            acks.append({'topic':topic,'partition':meta.partition,'offset':meta.offset,
                         'doc_id':f'{topic}:{meta.partition}:{meta.offset}','value':raw,
                         'ack_ms':int(time.time()*1000),'delivery_ms':(time.monotonic()-start)*1000})
    finally:p.close(timeout=15)
    return acks


def observe(positions):
    if len(positions)>200:raise ValueError('Kafka observe budget exceeded')
    consumer=KafkaConsumer(bootstrap_servers=BOOTSTRAP.split(','),enable_auto_commit=False,request_timeout_ms=15000,
                           api_version_auto_timeout_ms=10000)
    result=[]
    try:
        grouped={}
        for pos in positions:grouped.setdefault((pos['topic'],int(pos['partition'])),set()).add(int(pos['offset']))
        for (topic,partition),offsets in grouped.items():
            if topic not in TOPICS:raise ValueError('Foreign topic forbidden')
            tp=TopicPartition(topic,partition);consumer.assign([tp]);consumer.seek(tp,min(offsets))
            deadline=time.monotonic()+15
            while offsets and time.monotonic()<deadline:
                for records in consumer.poll(timeout_ms=300,max_records=250).values():
                    for record in records:
                        if record.offset not in offsets:continue
                        offsets.remove(record.offset)
                        result.append({'topic':topic,'partition':partition,'offset':record.offset,
                            'kafka_timestamp_ms':record.timestamp,'value':record.value.decode('utf-8')})
    finally:consumer.close()
    return result


def end_offsets():
    consumer=KafkaConsumer(bootstrap_servers=BOOTSTRAP.split(','),enable_auto_commit=False)
    try:
        parts=[TopicPartition(topic,p) for topic in TOPICS for p in (consumer.partitions_for_topic(topic) or [])]
        offsets=consumer.end_offsets(parts)
        return {topic:{str(tp.partition):offset for tp,offset in offsets.items() if tp.topic==topic}
                for topic in TOPICS}
    finally:consumer.close()


def invoke(op,args):
    global FIXTURE
    if op=='http':return http(args.get('method','GET'),args['url'],args.get('body'),min(args.get('timeout',20),30))
    if op=='publish':return publish(args['messages'])
    if op=='observe':return observe(args['positions'])
    if op=='end_offsets':return end_offsets()
    if op=='hdfs_records':return hdfs_records()
    if op=='checkpoints':return checkpoints()
    if op=='hdfs':
        if args.get('op','LISTSTATUS') not in ('LISTSTATUS','GETCONTENTSUMMARY','GETFILESTATUS'):raise ValueError('Read only HDFS RPC')
        return hdfs(args['path'],args.get('op','LISTSTATUS'))
    if op=='topics_init':
        admin=KafkaAdminClient(bootstrap_servers=BOOTSTRAP.split(','),request_timeout_ms=15000)
        try:
            existing=set(admin.list_topics())
            wanted=[NewTopic(topic,3,3,topic_configs={'min.insync.replicas':'2','retention.ms':'7200000'}) for topic in TOPICS if topic not in existing]
            if wanted:admin.create_topics(wanted,timeout_ms=30000)
            return {'topics':admin.describe_topics(TOPICS)}
        finally:admin.close()
    if op=='crawl_apple':
        import requests,src_apple
        FIXTURE=args['fixture']
        src_apple.FEED='http://qa-agent:8899/fixtures/apple/{country}/{limit}/songs.json'
        capture=int(time.time()*1000)
        with requests.Session() as session:records=list(src_apple.crawl_charts(session,capture))
        messages=[]
        for topic,key,record in records:
            record.update(test_run_id=RUN,synthetic=True)
            messages.append({'topic':topic,'key':key,'record':record})
        return {'capture_ms':capture,'acks':publish(messages),'records':[m['record'] for m in messages]}
    if op=='listen_parallel':
        bodies=args['bodies'];workers=min(int(args.get('workers',2)),4,len(bodies))
        if len(bodies)>40 or not bodies:raise ValueError('Bounded load:1..40 requests')
        barrier=threading.Barrier(workers)
        def work(index):
            if index<workers:barrier.wait(timeout=10)
            return http('POST','http://music-api:8000/api/listen',bodies[index])
        started=time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:responses=list(pool.map(work,range(len(bodies))))
        return {'responses':responses,'wall_seconds':time.monotonic()-started,'workers':workers}
    if op=='versions':
        import importlib.metadata,platform
        return {'python':platform.python_version(),'packages':{p:importlib.metadata.version(p) for p in ['requests','kafka-python','pyarrow']}}
    raise ValueError('Unsupported observer operation')


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        data=FIXTURE if self.path.startswith('/fixtures/apple/') else {'ready':True,'run_id':RUN}
        self.reply(200,data)
    def do_POST(self):
        if self.path!='/invoke':return self.reply(404,{})
        length=int(self.headers.get('Content-Length','0'))
        if length>512000:return self.reply(413,{'error':'request budget exceeded'})
        try:
            request=json.loads(self.rfile.read(length))
            with SLOTS:result=invoke(request['op'],request.get('args',{}))
            self.reply(200,{'result':result})
        except Exception as exc:self.reply(500,{'error':type(exc).__name__,'detail':str(exc)[:1200]})
    def reply(self,status,data):
        body=json.dumps(data,ensure_ascii=False,default=str,allow_nan=False).encode('utf-8')
        self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def log_message(self,*args):pass


if __name__=='__main__':ThreadingHTTPServer(('0.0.0.0',8899),Handler).serve_forever()
