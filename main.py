"""
RUN WITH SPARK SHELL:
./pyspark \
--packages com.databricks:spark-xml_2.12:0.14.0\
--py-files /media/sf_cartella_condivisa/progetto/Big_pyspark/utils.zip

RUN WITH SPARK SUBMIT:
spark-submit\
--packages com.databricks:spark-xml_2.12:0.14.0\
--py-files /media/sf_cartella_condivisa/progetto/Big_pyspark/utils.zip\
/media/sf_cartella_condivisa/progetto/Big_pyspark/main.py
"""

from pyspark.sql.session import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark import Row

from pm4py.objects.log.importer.xes.variants import iterparse as xes_importer
from pm4py.objects.petri_net.importer.variants import pnml as pnml_importer
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments
import pm4py.algo.discovery.causal.variants.heuristic as cr_discovery

import time

from utils import logRdd, genericFunctions

spark = SparkSession.builder \
    .appName("BIG_Pyspark") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", 4) \
    .config("spark.driver.cores", 1) \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0") \
    .getOrCreate()

start = time.time()

rootHdfsPath = 'hdfs://localhost:9000/'
rootLocalPathPnml = '/media/sf_cartella_condivisa/progetto/Big_pyspark/pnml/'
rootLocalPathXes = '/media/sf_cartella_condivisa/progetto/Big_pyspark/xes/'
xes = ['toyex.xes',
       'testBank2000NoRandomNoise.xes',
       'andreaHelpdesk.xes',
       'andrea_bpi12full.xes'
       ]
pnml = ['toyex_petriNet.pnml',
        'testBank2000NoRandomNoise_petriNet.pnml',
        'andreaHelpdesk_petriNet.pnml',
        'andrea_bpi12full_petriNet.pnml'
        ]
test = 3

# CAUSAL RELATIONS (cr)
log = xes_importer.import_log(rootLocalPathXes + xes[test])
dfg = dfg_discovery.apply(log)
CR = cr_discovery.apply(dfg)
cr = [key for key, val in CR.items() if val > 0.8]

# ALIGNMENTS (df_alignments)
log_rdd = logRdd.create_rdd_from_xes(spark, rootHdfsPath + xes[test])
net, im, fm = pnml_importer.import_net(rootLocalPathPnml + pnml[test])
rdd_alignments = log_rdd.map(lambda t: Row(t.attributes['concept:name'], alignments.apply(t, net, im, fm)['alignment']))
df_alignments = rdd_alignments.toDF().withColumnRenamed('_1', 'trace_id').withColumnRenamed('_2', 'alignments')
print("-------------- ALIGNMENTS ---------------\n")
df_alignments.show()

# TRACES (df_traces)
df_traces = genericFunctions.create_df_from_xes(spark, rootHdfsPath + xes[test])
print("-------------- TRACES ---------------\n")
df_traces.show()

# EXEC_TIME
end = time.time()
print("TEMPO IMPIEGATO: " + str((end - start)) + " secondi")