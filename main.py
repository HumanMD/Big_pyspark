"""
RUN WITH SPARK SHELL:
./pyspark \
--executor-cores 2 \
--driver-cores 2
--driver-memory 4g \
--executor-memory 4g \
--packages com.databricks:spark-xml_2.12:0.14.0 \
--py-files /media/sf_cartella_condivisa/progetto/Big_pyspark/utils.zip \

RUN WITH SPARK SUBMIT:
spark-submit --executor-cores 2 --driver-cores 2 --driver-memory 4g --executor-memory 4g --packages com.databricks:spark-xml_2.12:0.14.0 --py-files /media/sf_cartella_condivisa/progetto/Big_pyspark/utils.zip /media/sf_cartella_condivisa/progetto/Big_pyspark/main.py
"""

from pyspark.sql.session import SparkSession
from pyspark import Row

from pm4py.objects.petri_net.importer.variants import pnml as pnml_importer
from pm4py.algo.conformance.alignments.petri_net import algorithm as alig

from utils import logRdd as lr
from utils import genericFunctions as gf

import time

spark = SparkSession.builder \
    .appName("BIG_Pyspark") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

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

# TRACES AND ALIGNMENTS (df)
net, im, fm = pnml_importer.import_net(rootLocalPathPnml + pnml[test])
df = lr \
    .create_rdd_from_xes(spark, rootHdfsPath + xes[test]) \
    .map(lambda t: Row(t.attributes['concept:name'], gf.getEvents(t), alig.apply(t, net, im, fm)['alignment'])) \
    .toDF() \
    .withColumnRenamed('_1', 'trace_id') \
    .withColumnRenamed('_2', 'trace') \
    .withColumnRenamed('_3', 'alignment')

print("-------------- TRACES AND ALIGNMENTS ---------------\n")
df.show()

# TRACE COUNT
print("-------------- TRACE_COUNT ---------------\n\n" + str(df.count()))

# EXEC_TIME
end = time.time()
print("-------------- EXEC_TIME ---------------\n\n" + str((end - start)) + " secondi")
