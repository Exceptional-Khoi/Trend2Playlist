import pytest


def case(identifier,title,order,*,priority='P0',timeout=180,services='API,Kafka,Spark,HDFS,Elasticsearch',
         expected='Traceable identities preserved and independent oracle satisfied',kind='e2e',destructive=False):
    def decorate(fn):
        fn=pytest.mark.cloud_case(id=identifier,title=title,order=order,priority=priority,timeout=timeout,
            services=services,expected=expected,kind=kind,destructive=destructive)(fn)
        fn=pytest.mark.cloud(fn);fn=getattr(pytest.mark,kind)(fn)
        if destructive:fn=pytest.mark.destructive(fn)
        return fn
    return decorate
