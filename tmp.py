# Avviare spark shell con il seguente comando :
# ./pyspark \
# --packages com.databricks:spark-xml_2.12:0.14.0 \
# --py-files /media/sf_cartella_condivisa/progetto/Big_pyspark/conformancechecking4spark.zip


# avviare con spark-submit
# spark-submit\
# --packages com.databricks:spark-xml_2.12:0.14.0\
# --py-files /media/sf_cartella_condivisa/progetto/Big_pyspark/conformancechecking4spark.zip\
# /media/sf_cartella_condivisa/progetto/Big_pyspark/BIG_pyspark.py

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

from pyspark.sql import *
from pyspark.sql.types import *
from pm4py.objects.petri_net.importer.variants import pnml as pnml_importer
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments
import pyspark.sql.functions as F
import pm4py.algo.discovery.causal.variants.heuristic as cr_discovery
import time

from conformancechecking4spark import log_rdd

start = time.time()

rootHdfsPath = 'hdfs://localhost:9000/'
rootLocalPath = '/media/sf_cartella_condivisa/progetto/Big_pyspark/'
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

log = log_rdd.create_from_xes(spark, rootHdfsPath + xes[test])
net, im, fm = pnml_importer.import_net(rootLocalPath + pnml[test])

rdd_alignments = log.map(lambda t: Row(t.attributes['concept:name'], alignments.apply(t, net, im, fm)['alignment']))
df = rdd_alignments.toDF()
df.show()

# TODO ora che non si punta sull'alignement devo integrare tmp.py con Big_pyspark
# TODO devo eliminare cio che non serve