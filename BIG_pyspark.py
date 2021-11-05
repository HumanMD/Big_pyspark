# from pyspark.context import SparkContext
# from pyspark.sql.session import SparkSession
from pyspark.sql import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DateType
import time as tempo
from pm4py.objects.log.importer.xes.variants import iterparse as xes_importer
from pm4py import conformance as conf
from pm4py.objects.petri_net.importer.variants import pnml as pnml_importer
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
import pm4py.algo.discovery.causal.variants.heuristic as cr_discovery

# sc = SparkContext.getOrCreate()
# spark = SparkSession(sc)
path_pnml = "/media/sf_cartella_condivisa/progetto/Big_pyspark/toyex_petriNet.pnml"
path_xes = "/media/sf_cartella_condivisa/progetto/Big_pyspark/toyex.xes"
tempo_inizio = tempo.time()

net, im, fm = pnml_importer.import_net(path_pnml)

# IMPORTA EVENTLOG DA FILE.XES
log = xes_importer.import_log(path_xes)
# CONFORMANCE CHECKING
check = conf.conformance_diagnostics_token_based_replay(log, net, im, fm)
# DIRECT FOLLOW GRAPH FROM LOG
dfg = dfg_discovery.apply(log)
# extract a dictionary with all causal relation as key and a value (-1,1) indicating how strong is the relation
CR = cr_discovery.apply(dfg)

cr = []
# add in cr array all the relation with value grater than 0.8 (filter)
# exemple of key : ('B','J')
for key, val in CR.items():
    if val > 0.8:
        cr.append(key)

print("\nTempo impiegato per trovare tutte le relazioni causali ed i parallelismi")
print(tempo.time() - tempo_inizio)
print("\n Numero di tracce totali nel file: " + str(len(log)))

new_df = []
i = 0

# popolare array per costruzione rdd -> dataframe
for trace in log:
    if (check[i]['trace_is_fit']):
        id_traccia = trace.attributes['concept:name']
        activities = []
        separator = ","
        for event in trace:
            activities.append(event['concept:name'])
            # activity = event['concept:name']
            # time = event['time:timestamp']
            # new_df.append((id_traccia, activity, time))
        new_df.append((id_traccia, separator.join(activities)))
    i += 1

print("\nTempo per conf check e prendere le tracce che fittano:")
print(tempo.time() - tempo_inizio)

schema = StructType([
    StructField('Trace_ID', StringType(), True),
    StructField('Trace', StringType(), True),
    # StructField('Time', DateType(), True)
])

rdd = spark.sparkContext.parallelize(new_df)
dataframe_fit = spark.createDataFrame(rdd, schema)

dataframe_fit.groupBy('Trace').count().show()  # get distinct trace and cardinality

tot_tracce = dataframe_fit.select("Trace_ID").count()
# tot_tracce = len(id_tracce)
print("\n Numero di tracce che hanno fittato: " + str(tot_tracce))


# TODO adattare le funzioni per usare dataframe
def ExtractInstanceGraph(trace, cr):
    V = []
    W = []
    id = 1
    for event in trace:
        V.append((id, event.get("concept:name")))
        id += 1
    # print("IG")
    for i in range(len(V)):
        for k in range(i, len(V)):
            e1 = V[i]
            e2 = V[k]
            if (e1[1], e2[1]) in cr:
                flag_e1 = True
                for s in range(i + 1, k):
                    e3 = V[s]
                    if (e1[1], e3[1]) in cr:
                        flag_e1 = False
                flag_e2 = True
                for s in range(i + 1, k):
                    e3 = V[s]
                    if (e3[1], e2[1]) in cr:
                        flag_e2 = False

                if flag_e1 or flag_e2:
                    W.append((e1, e2))
    return V, W
    # print(V)
    # print(W)
