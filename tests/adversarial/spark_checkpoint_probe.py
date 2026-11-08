"""Executed only inside the campaign-owned Spark container, temporary paths only."""
import json
from pathlib import Path
import tempfile
from pyspark.sql import SparkSession

spark=SparkSession.builder.master('local[2]').appName('qa-checkpoint').config('spark.sql.shuffle.partitions','2').getOrCreate()
query=None
try:
    with tempfile.TemporaryDirectory(prefix='qa-checkpoint-') as tmp:
        root=Path(tmp);incoming=root/'in';incoming.mkdir();out=root/'out';checkpoint=root/'checkpoint'
        (incoming/'a.json').write_text('{"id":"a"}\n')
        def start():return spark.readStream.schema('id string').json(str(incoming)).writeStream.format('parquet').option('checkpointLocation',str(checkpoint)).start(str(out))
        query=start();query.processAllAvailable();query.stop()
        (incoming/'b.json').write_text('{"id":"b"}\n')
        query=start();query.processAllAvailable();query.stop()
        actual=sorted(r.id for r in spark.read.parquet(str(out)).collect())
        assert actual==['a','b'],actual
        print('QA_CHECKPOINT_RESULT='+json.dumps(actual),flush=True)
finally:
    if query and query.isActive:query.stop()
    spark.stop()
