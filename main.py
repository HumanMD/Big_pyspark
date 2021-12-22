"""
RUN WITH SPARK SHELL:
./pyspark --packages com.databricks:spark-xml_2.12:0.14.0 --py-files /media/sf_cartella_condivisa/progetto/Big_pyspark/utils.zip

RUN WITH SPARK SUBMIT:
spark-submit --packages com.databricks:spark-xml_2.12:0.14.0 --py-files /media/sf_cartella_condivisa/progetto/Big_pyspark/utils.zip /media/sf_cartella_condivisa/progetto/Big_pyspark/main.py
"""

from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
import pm4py.algo.discovery.causal.variants.heuristic as cr_discovery
from pm4py.objects.petri_net.importer import importer as pnml_importer
from pm4py.algo.conformance.alignments.petri_net import algorithm as alig

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import Row
import pyspark.sql.functions as F

import time

from utils.genericFunctions import getEvents, create_V, create_W, create_D_or_I, irregularGraphRepairing, writeOnFile, \
    getShowString
from utils.logRdd import create_rdd_from_xes
from utils.schema import V_schema, W_schema, D_I_schema

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

hdfsPath = 'hdfs://localhost:9000/'
localPath = '/media/sf_cartella_condivisa/progetto/Big_pyspark/'
xesFileName = ['toyex.xes',
               'testBank2000NoRandomNoise.xes',
               'andreaHelpdesk.xes',
               ]
pnmlFileName = ['toyex_petriNet.pnml',
                'testBank2000NoRandomNoise_petriNet.pnml',
                'andreaHelpdesk_petriNet.pnml',
                ]
outputFileNames = [
    'main_toyex_Wi',
    'main_testBank2000NoRandomNoise_Wi',
    'main_andreaHelpdesk_Wi',
]
test = 2
outputPath = localPath + 'output/' + outputFileNames[test]
start = time.time()

# ---- TRACES AND ALIGNMENTS (df) ---- #
# - extract parsed log from the xes file as rdd
# - for each row extract trace_id, trace_events and alignment
# - convert to dataframe and rename the columns
net, im, fm = pnml_importer.apply(localPath + 'pnml/' + pnmlFileName[test])
df = create_rdd_from_xes(spark, hdfsPath + xesFileName[test]) \
    .map(lambda t: Row(t.attributes['concept:name'], getEvents(t), alig.apply(t, net, im, fm)['alignment'])) \
    .toDF() \
    .withColumnRenamed('_1', 'trace_id') \
    .withColumnRenamed('_2', 'trace') \
    .withColumnRenamed('_3', 'alignment')

# rdd = create_rdd_from_xes(spark, hdfsPath + xesFileName[test]) \
#     .map(lambda t: Row(t.attributes['concept:name'], getEvents(t), alig.apply(t, net, im, fm)['alignment']))
# out = rdd.collect()
# for row in out:
#     tmp = 'TRACE_ID: ' + row[0] + ', TRACE: ' + str(row[1])
#     tmp = 'TRACE_ID: ' + row[0] + ', ALIGNMENT: ' + str(row[2])
#     writeOnFile('/media/sf_cartella_condivisa/progetto/Big_pyspark/output/' + outputFileNames[test], tmp)
# writeOnFile('/media/sf_cartella_condivisa/progetto/Big_pyspark/output/' + outputFileNames[test], len(out))

# ---- CAUSAL RELATIONS (cr) ---- #
# - extract the log with pm4py
# - extract direct follow graph from the log
# - extract the causal relations from the direct follow graph
# - filter the causal relations with value grater than 0.8
log = xes_importer.apply(localPath + 'xes/' + xesFileName[test])
dfg = dfg_discovery.apply(log)
CR = cr_discovery.apply(dfg)
cr = [key for key, val in CR.items() if val > 0.8]

# writeOnFile('/media/sf_cartella_condivisa/progetto/Big_pyspark/output/' + outputFileNames[test], len(cr))
# writeOnFile('/media/sf_cartella_condivisa/progetto/Big_pyspark/output/' + outputFileNames[test], cr)


# ---- UDF FUNCTIONS ---- #
# definition of udf for:
# - nodes from trace (V)
# - arches from nodes and causal relations filtered (W)
# - deletion to apply from alignments for each trace (D)
# - insertion to apply from alignments for each trace (I)
# - creation of unrepaired instance graph and, deletion and insertion repair, creation of repaired instance graph
udf_create_V = F.udf(lambda trace: create_V(trace), V_schema)
udf_create_W = F.udf(lambda V: create_W(cr, V), W_schema)
udf_create_D = F.udf(lambda alignments: create_D_or_I(alignments, 'D'), D_I_schema)
udf_create_I = F.udf(lambda alignments: create_D_or_I(alignments, 'I'), D_I_schema)
udf_irregularGraphRepairing = F.udf(lambda V, W, D, I: irregularGraphRepairing(V, W, D, I, cr, outputPath), W_schema)

# ---- FINAL DATAFRAME ---- #
final_df = df.withColumn('V', udf_create_V('trace')) \
    .withColumn('W', udf_create_W('V')) \
    .withColumn("D", udf_create_D('alignment')) \
    .withColumn("I", udf_create_I('alignment')) \
    .withColumn('Wi', udf_irregularGraphRepairing('V', 'W', 'D', 'I'))
# EOFError happens here for all xes file except toyex

out = final_df.select('trace_id', 'Wi').rdd.collect()

for row in out:
    if row[1] is not None:
        tmp = 'TRACE_ID: ' + row[0] + ', Wi: ' + str([((a, b), (c, d)) for ((a, b), (c, d)) in row[1]])
        # print(tmp)
        writeOnFile('/media/sf_cartella_condivisa/progetto/Big_pyspark/output/' + outputFileNames[test], tmp)

# showFinalDf = getShowString(final_df, n=5, truncate=False, vertical=False)
# writeOnFile(outputPath, showFinalDf)

# ---- EXEC_TIME ---- #
# endString = "-------------- EXEC_TIME ---------------\n\n" + str((time.time() - start)) + " secondi"
# writeOnFile(outputPath, endString)
# print('INSTANCE GRAPHS, FINAL DATAFRAME AND EXEC TIME AVAILABLE ON ' + outputPath)
