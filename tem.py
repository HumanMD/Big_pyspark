from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

df = spark.read.format("xml") \
    .option("rowTag", "trace") \
    .option("valueTag", "anyName") \
    .load("hdfs://localhost:9000/toyex.xes") \
    .withColumn('trace_id', F.col("string")._value) \
    .withColumn('trace', F.col("event").string) \
    .select('trace_id', F.explode('trace').alias('trace')) \
    .withColumn('trace', F.col("trace")[1]._value)\
    .groupBy('trace_id')\
    .agg(F.collect_list('trace').alias('trace'))\
    .orderBy('trace_id')

df.show(100)
