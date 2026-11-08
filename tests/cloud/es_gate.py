"""Real HTTP fault proxy to owned ES, controlled through a run-local flag file."""
import json
import os
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path


class Gate(BaseHTTPRequestHandler):
    def handle_request(self):
        mode=Path('/tmp/qa-es-fault').read_text().strip() if Path('/tmp/qa-es-fault').exists() else ''
        if mode=='unavailable':
            print('QA_ES_FAULT '+json.dumps({'time_ms':int(time.time()*1000),'method':self.command,'path':self.path,'status':503}),flush=True)
            data=b'{"error":"qa-injected-unavailable"}';self.send_response(503);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data);return
        if mode=='latency':
            # Explicit bounded injected network delay, not readiness synchronization.
            time.sleep(0.4)
        size=int(self.headers.get('Content-Length','0'))
        body=self.rfile.read(size) if size else None
        if 'chunked' in self.headers.get('Transfer-Encoding','').lower():
            chunks=[];total=0
            while True:
                length=int(self.rfile.readline().split(b';',1)[0].strip(),16)
                if not length:
                    self.rfile.readline();break
                total+=length
                if total>8*1024*1024:raise ValueError('Proxy body limit')
                chunks.append(self.rfile.read(length));self.rfile.read(2)
            body=b''.join(chunks)
        headers={k:v for k,v in self.headers.items() if k.lower() in ('content-type','accept','content-encoding')}
        request=urllib.request.Request('http://elasticsearch:9200'+self.path,data=body,method=self.command,headers=headers)
        try:response=urllib.request.urlopen(request,timeout=30)
        except urllib.error.HTTPError as exc:response=exc
        data=response.read();self.send_response(response.status)
        for key,value in response.headers.items():
            if key.lower() not in ('connection','transfer-encoding','content-length','server','date'):self.send_header(key,value)
        self.send_header('Content-Length',str(len(data)));self.end_headers()
        if self.command!='HEAD':self.wfile.write(data)
    do_GET=do_POST=do_PUT=do_DELETE=do_HEAD=handle_request
    def log_message(self,*args):pass


if __name__=='__main__':ThreadingHTTPServer(('0.0.0.0',9200),Gate).serve_forever()
