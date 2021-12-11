"""
RUN WITH SPARK SHELL:
./pyspark --executor-cores 3 --packages com.databricks:spark-xml_2.12:0.14.0 --py-files /media/sf_cartella_condivisa/progetto/Big_pyspark/utils.zip

RUN WITH SPARK SUBMIT:
spark-submit --executor-cores 3 --packages com.databricks:spark-xml_2.12:0.14.0 --py-files /media/sf_cartella_condivisa/progetto/Big_pyspark/utils.zip /media/sf_cartella_condivisa/progetto/Big_pyspark/main.py
"""
from pm4py.objects.log.importer.xes import importer as xes_importer
# from pm4py.objects.log.importer.xes.variants import iterparse as xes_importer
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
import pm4py.algo.discovery.causal.variants.heuristic as cr_discovery
from pm4py.objects.petri_net.importer import importer as pnml_importer
# from pm4py.objects.petri_net.importer.variants import pnml as pnml_importer
from pm4py.algo.conformance.alignments.petri_net import algorithm as alig

from pyspark.sql.session import SparkSession
from pyspark import Row
import pyspark.sql.functions as F

from utils import logRdd as lr
from utils import genericFunctions as gf
from utils.genericFunctions import create_V, create_W, irregularGraphRepairing, \
    onFileFinalIG, create_D_or_I
from utils.schema import V_schema, W_schema, D_I_schema

import time

spark = SparkSession.builder \
    .appName("BIG_Pyspark") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

start = time.time()

rootHdfsPath = 'hdfs://localhost:9000/'
rootPath = '/media/sf_cartella_condivisa/progetto/Big_pyspark/'
rootLocalPathPnml = rootPath + 'pnml/'
rootLocalPathXes = rootPath + 'xes/'
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
outputFileNames = [
    'toyex_IG',
    'testBank2000NoRandomNoise_IG',
    'andreaHelpdesk_IG',
    'andrea_bpi12full_IG'
]

test = 1

# TRACES AND ALIGNMENTS (df)
net, im, fm = pnml_importer.apply(rootLocalPathPnml + pnml[test])
df = lr \
    .create_rdd_from_xes(spark, rootHdfsPath + xes[test]) \
    .map(lambda t: Row(t.attributes['concept:name'], gf.getEvents(t), alig.apply(t, net, im, fm)['alignment'])) \
    .toDF() \
    .withColumnRenamed('_1', 'trace_id') \
    .withColumnRenamed('_2', 'trace') \
    .withColumnRenamed('_3', 'alignment')

# CAUSAL RELATIONS (cr)
log = xes_importer.apply(rootLocalPathXes + xes[test])
dfg = dfg_discovery.apply(log)
CR = cr_discovery.apply(dfg)
cr = [key for key, val in CR.items() if val > 0.8]

udf_create_V = F.udf(lambda trace: create_V(trace), V_schema)
udf_create_W = F.udf(lambda V: create_W(cr, V), W_schema)
udf_create_D = F.udf(lambda alignments: create_D_or_I(alignments, 'D'), D_I_schema)
udf_create_I = F.udf(lambda alignments: create_D_or_I(alignments, 'I'), D_I_schema)
udf_irregularGraphRepairing = F.udf(lambda V, W, D, I: irregularGraphRepairing(V, W, D, I, cr), W_schema)

# FINAL DATAFRAME
final_df = df.withColumn('V', udf_create_V('trace')) \
    .withColumn('W', udf_create_W('V')) \
    .withColumn("D", udf_create_D('alignment')) \
    .withColumn("I", udf_create_I('alignment')) \
    .withColumn('Wi', udf_irregularGraphRepairing('V', 'W', 'D', 'I')) \
    .select('trace_id', 'V', 'Wi')

# INSTANCE GRAPHS
onFileFinalIG(rootPath + 'output/', outputFileNames[test], final_df)

# EXEC_TIME
end = time.time()
print("-------------- EXEC_TIME ---------------\n\n" + str((end - start)) + " secondi")
