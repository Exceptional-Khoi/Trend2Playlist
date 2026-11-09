import datetime
import json
import time
import urllib.parse

from .safety import poll


class Data:
    def __init__(self,stack):
        self.stack=stack;self.prefix=stack.case_id(stack.current.lower());self.now=int(time.time()*1000)
        self.day=datetime.datetime.now(datetime.timezone.utc).date()
        self.as_of=(self.day+datetime.timedelta(days=1)).isoformat()

    def track(self,suffix='track'):return self.prefix+'-'+suffix

    def chart(self,track,rank=1,size=2,day=None,country='VN',**extra):
        period=(day or self.day).isoformat() if not isinstance(day,str) else day
        row={'record_type':'chart_entry','source':'spotify','data_provider':'spotify_kworb',
             'chart_id':self.prefix+'-daily_'+country,'chart_scope':country,'country_code':country,'period_type':'daily',
             'source_period':period,'snapshot_id':self.prefix+'-'+country+'-'+period,'crawled_ms':self.now,
             'track_key':track,'platform_id':track,'title':'QA Đêm 🎵 '+track,'artists':['QA Artist'],
             'genres':['Pop'],'rank':rank,'chart_size':size,'metric_value':100,'duration_s':200,
             'test_run_id':self.stack.run_id,'synthetic':True}
        row.update(extra);return row

    def chart_rows(self,tracks,country='VN',day=None,**extra):
        return [self.chart(t,i,len(tracks),day,country,**extra) for i,t in enumerate(tracks,1)]

    def event(self,identifier,action='play',track=None,**extra):
        row={'event_id':self.prefix+'-'+identifier,'user_id':self.prefix+'-user','province_code':'ha-noi',
             'track_key':track or self.track(),'title':'QA Đêm 🎵','artists':['QA Artist'],'genre':'Pop',
             'source':'web','action':action,'ts_ms':self.now,'duration_ms':None,'listen_ms':None}
        row.update(extra);return row

    def messages(self,records,topic='music.charts',**extra):
        return [{'topic':topic,'key':r.get('track_key',self.prefix),'record':r,**extra} for r in records]

    def publish(self,records,topic='music.charts',**extra):return self.stack.publish(self.messages(records,topic,**extra))

    def insights(self,feature,**params):
        query={'as_of':self.as_of,'days':30,**params}
        return self.stack.http('/api/insights/'+feature+'?'+urllib.parse.urlencode(query))

    def ready(self,feature,predicate,**params):
        result=poll(lambda:self.insights(feature,**params),lambda r:r['status']==200 and predicate(r['body']),
                    timeout=150,interval=3,guard=self.stack.guard)
        self.stack.trace('api','PASS',result);return result['body']

    def seed_catalog(self,track=None):
        track=track or self.track()
        row={'track_key':track,'title':'QA metadata '+self.prefix,'artists':['QA Artist'],'genre':'Pop',
             'duration_s':200,'qa_test_run_id':self.stack.run_id}
        result=self.stack.http('/music-tracks/_doc/'+urllib.parse.quote(track,safe='')+'?refresh=true',method='PUT',body=row,service='elasticsearch')
        assert result['status'] in (200,201)
        self.stack.trace('catalog_precondition','SEEDED',{'index':'music-tracks','id':track,'body':row,
                         'note':'Controlled metadata precondition, not proof of crawler-to-batch catalog'})
        return track
