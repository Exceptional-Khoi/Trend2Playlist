"""Post-storage-fault proof must not reuse pre-fault observer bytes/rows."""
FRESH_HDFS_CODE = '''import agent,json,signal,time
signal.alarm(100)
assert not agent.HDFS_CACHE and not agent.PARQUET_CACHE
started=int(time.time()*1000)
rows=agent.hdfs_records()
print(json.dumps(dict(rows,observer='fresh-process',started_ms=started,finished_ms=int(time.time()*1000)),default=str))
'''


def assert_fresh_acks(acks, result):
    assert result.get('observer') == 'fresh-process', 'Storage proof must use a fresh observer process'
    rows=result['rows']
    for ack in acks:
        matches=[row for row in rows if (row['topic'],row['partition'],row['offset']) ==
                 (ack['topic'],ack['partition'],ack['offset'])]
        assert len(matches)==1, 'Acknowledged identity missing or duplicated in fresh committed storage'
        assert matches[0]['value']==ack['value'], 'Fresh committed storage payload differs from broker acknowledgement'
