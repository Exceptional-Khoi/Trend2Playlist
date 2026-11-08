"""Real state store with the unmodified production callback and an early-return stub."""
import ast
import json
from pathlib import Path
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

text=Path('/qa-source/spark/speed_layer.py').read_text()
node=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='recommend_batch')
namespace={'_recommend':lambda *a:None}
exec(compile(ast.Module(body=[node],type_ignores=[]),'/qa-source/spark/speed_layer.py','exec'),namespace)
spark=SparkSession.builder.master('local[2]').appName('qa-state-consumption').config('spark.sql.shuffle.partitions','4').getOrCreate()
query=None
try:
    with tempfile.TemporaryDirectory(prefix='qa-state-') as tmp:
        root=Path(tmp);incoming=root/'in';incoming.mkdir()
        def add(name):
            (incoming/(name+'.json')).write_text('\n'.join(json.dumps({'user_id':str(i),'ts_ms':1790812800000,
                'track_key':'x','action':'play','title':'X','province_code':'ha-noi'}) for i in range(32)))
        add('a')
        events=(spark.readStream.schema('user_id string, ts_ms long, track_key string, action string, title string, province_code string')
                .json(str(incoming)).withColumn('ts',F.to_timestamp(F.col('ts_ms')/1000)))
        sessions=(events.withWatermark('ts','2 minutes').groupBy(F.window('ts','30 minutes','5 minutes'),'user_id')
                  .agg(F.collect_list(F.struct('ts_ms','track_key','action','title')).alias('hist'),F.last('province_code').alias('province_code')))
        query=(sessions
               .writeStream.outputMode('update').option('checkpointLocation',str(root/'checkpoint'))
               .foreachBatch(namespace['recommend_batch']).start())
        query.processAllAvailable()
        add('b');query.processAllAvailable()
        print('QA_STATE_RESULT=two_batches_completed',flush=True)
finally:
    if query and query.isActive:query.stop()
    spark.stop()
