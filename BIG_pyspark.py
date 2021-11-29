# from pyspark.context import SparkContext
# from pyspark.sql.session import SparkSession

# Avviare spark shell con il seguente comando :
# ./pyspark --packages com.databricks:spark-xml_2.12:0.14.0

from pyspark.sql import *
from pyspark.sql.types import *
from pm4py.objects.log.importer.xes.variants import iterparse as xes_importer
from pm4py.objects.petri_net.importer.variants import pnml as pnml_importer
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments

import pyspark.sql.functions as F
import pm4py.algo.discovery.causal.variants.heuristic as cr_discovery
import time

# sc = SparkContext.getOrCreate()
# spark = SparkSession(sc)
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


# net, im, fm = pnml_importer.import_net(rootLocalPath + pnml[test])  # IMPORT PETRI NET
# log = xes_importer.import_log(rootLocalPath + xes[test])  # IMPORT EVENT_LOG FROM FILE.XES
# dfg = dfg_discovery.apply(log)  # DIRECT FOLLOW GRAPH FROM LOG
# CR = cr_discovery.apply(dfg)  # CAUSAL RELATIONS AS KEY AND THEIR WEIGHT AS VALUE

# todo prova a passare cr come vettore alle funzioni senza inserirlo nel dataframe
# cr = [key for key, val in CR.items() if val > 0.8]  # FILTER CAUSAL RELATIONS
# alignments_cr = []
#
# for trace in log:
#     trace_alignments = alignments.apply(trace, net, im, fm)['alignment']
#     id_trace = trace.attributes['concept:name']
#     alignments_cr.append((id_trace, cr, trace_alignments))
#
# df_alignments_cr_schema = StructType([
#     StructField('alignments_cr_trace_id', StringType(), True),
#     StructField('causal_relations', ArrayType(
#         StructType(
#             [
#                 StructField('cr_strart', StringType()),
#                 StructField('cr_end', StringType()),
#             ]
#         )
#     ), True),
#     StructField('alignments', ArrayType(
#         StructType(
#             [
#                 StructField('on_log', StringType()),
#                 StructField('on_model', StringType()),
#             ]
#         )
#     ), True),
# ])  # SCHEMA FOR INITIAL DATAFRAME [ Trace_ID | Causal_relation | Alignments ]
#
# rdd_alignments_cr = spark.sparkContext.parallelize(alignments_cr)
# df_alignments_cr = spark.createDataFrame(rdd_alignments_cr, df_alignments_cr_schema)


# ---- FUNCTIONS ----- #

def checkAttribute(df, col_name):
    try:
        df.select(col_name)
        return True
    except:
        return False


def isArray(df, col_name):
    try:
        df.select(F.explode(F.col(col_name)))
        return True
    except:
        return False


# ---- CREATION OF INITIAL DF ---- #

df = spark.read.format('xml') \
    .option('rowTag', 'trace') \
    .option('valueTag', 'anyName') \
    .load(rootHdfsPath + xes[test])

intero = checkAttribute(df, 'int')
stringa = checkAttribute(df, 'string')
floatt = checkAttribute(df, 'float')
data = checkAttribute(df, 'date')
boleano = checkAttribute(df, 'boolean')
contenitore = checkAttribute(df, 'container')
lista = checkAttribute(df, 'list')

# In questa parte del codice cerco di semplificare la struttura del dataset per permette una facile estrazione dei valori
# che mi interessano. Per ogni attributo controllo che sia presente all'interno dello schema.
# Una volta che ho la certezza che sia presente, controllo se sia un array.
# Se lo è, lo scoppio e lo inserisco all'interno del datasetpulito.
# Sennò carico direttamente la colonna così come è presente all'interno del dataset di origine.

initial_df = df
print('DATAFRAME INIZIALE (initial_df):\n')
initial_df.printSchema()
initial_df.show()

if stringa:
    if isArray(df, 'string'):
        df = df.withColumn('String', F.explode(F.col('string')))
    else:
        df = df.withColumn('String', F.col('string'))

if intero:
    if isArray(df, 'int'):
        df = df.withColumn('Int', F.explode(F.col('int')))
    else:
        df = df.withColumn('Int', F.col('int'))

if floatt:
    if isArray(df, 'float'):
        df = df.withColumn('Float', F.explode(F.col('float')))
    else:
        df = df.withColumn('Float', F.col('float'))

if data:
    if isArray(df, 'date'):
        df = df.withColumn('Date', F.explode(F.col('date')))
    else:
        df = df.withColumn('Date', F.col('date'))

if boleano:
    if isArray(df, 'boolean'):
        df = df.withColumn('Boolean', F.explode(F.col('boolean')))
    else:
        df = df.withColumn('Boolean', F.col('boolean'))

cleaned_df = df
print('DATAFRAME PULITO (cleaned_df):\n')
cleaned_df.printSchema()
cleaned_df.show()

if stringa:
    df = df.withColumn('traceString_key', F.col('String._key')) \
        .withColumn('traceString_value', F.col('String._value')) \
        .drop('String')

if intero:
    df = df.withColumn('traceInt_key', F.col('Int._key')) \
        .withColumn('traceInt_value', F.col('Int._value')) \
        .drop('Int')

if floatt:
    df = df.withColumn('traceFloat_key', F.col('Float._key')) \
        .withColumn('traceFloat_value', F.col('Float._value')) \
        .drop('Float')

if data:
    df = df.withColumn('traceDate_key', F.col('Date._key')) \
        .withColumn('traceDate_value', F.col('Date._value')) \
        .drop('Date')

if boleano:
    df = df.withColumn('traceBoolean_key', F.col('Boolean._key')) \
        .withColumn('traceBoolean_value', F.col('Boolean._value')) \
        .drop('Boolean')

trace_log = df.drop('event')

print('INFO SU TRACCE DEL LOG (trace_log):\n')
trace_log.printSchema()
trace_log.show()

ids = trace_log.filter(trace_log.traceString_key == 'concept:name').select('traceString_value')

trace_count = ids.count()
print('NUMERO DI TRACCE:\n' + str(trace_count))

# In questa fase, ripeto lo stesso procedimento fatto per la tag "trace".
# Controllo tutte le tipologie di attributo e semplifico la struttura.
# Non sappiamo quanti tipologie di attributi abbiamo. Quindi devo analizzarli tutti quanti.
# Ho creato un dataset che contiene tutte le stringhe della traccia che contiene tutti gli eventi.

event_list = cleaned_df.withColumn('event', F.explode(F.col('event')).alias('event'))\
    .select('String._value', 'event')
event_list = event_list.join(ids, event_list._value == ids.traceString_value).select('_value', 'event')

print('INITIAL EVENT LIST:\n')
event_list.printSchema()
event_list.show()

intero_event = checkAttribute(event_list, 'event.int')
stringa_event = checkAttribute(event_list, 'event.string')
floatt_event = checkAttribute(event_list, 'event.float')
data_event = checkAttribute(event_list, 'event.date')
boleano_event = checkAttribute(event_list, 'event.boolean')
contenitore_event = checkAttribute(event_list, 'event.container')
lista_event = checkAttribute(event_list, 'event.list')

# In questa parte controllo se gli attributi sono degli array.

if stringa_event:
    if isArray(event_list, 'event.string'):
        event_list = event_list.withColumn('EventString', F.explode(F.col('event.string')))
    else:
        event_list = event_list.withColumn('EventString', F.col('event.string'))

if data_event:
    if isArray(event_list, 'event.date'):
        event_list = event_list.withColumn('EventDate', F.explode(F.col('event.date')))
    else:
        event_list = event_list.withColumn('EventDate', F.col('event.date'))

if floatt_event:
    if isArray(event_list, 'event.float'):
        event_list = event_list.withColumn('EventFloat', F.explode(F.col('event.float')))
    else:
        event_list = event_list.withColumn('EventFloat', F.col('event.float'))

if boleano_event:
    if isArray(event_list, 'event.boolean'):
        event_list = event_list.withColumn('EventBoolean', F.explode(F.col('event.boolean')))
    else:
        event_list = event_list.withColumn('EventBoolean', F.col('event.boolean'))

if intero_event:
    if isArray(event_list, 'event.int'):
        event_list = event_list.withColumn('EventInt', F.explode(F.col('event.int')))
    else:
        event_list = event_list.withColumn('EventInt', F.col('event.int'))

event_list = event_list.drop('event')

print('FINAL EVENT LIST:\n')
event_list.printSchema()
event_list.show()

# In questa parte vado a prendere la coppia chiave valore  degli attributi.

if stringa_event:
    event_list = event_list.withColumn('EventString_key', F.col('EventString._key')) \
        .withColumn('EventString_value', F.col('EventString._value')) \
        .drop('EventString')

if data_event:
    event_list = event_list.withColumn('EventDate_key', F.col('EventDate._key')) \
        .withColumn('EventDate_value', F.col('EventDate._value')) \
        .drop('EventDate')

if floatt_event:
    event_list = event_list.withColumn('EventFloat_key', F.col('EventFloat._key')) \
        .withColumn('EventFloat_value', F.col('EventFloat._value')) \
        .drop('EventFloat')

if intero_event:
    event_list = event_list.withColumn('EventInt_key', F.col('EventInt._key')) \
        .withColumn('EventInt_value', F.col('EventInt._value')) \
        .drop('EventInt')

if boleano_event:
    event_list = event_list.withColumn('EventBoolean_key', F.col('EventBoolean._key')) \
        .withColumn('EventBoolean_value', F.col('EventBoolean._value')) \
        .drop('EventBoolean')

traceEventLog = event_list.filter(event_list.EventString_key == 'concept:name')\
    .withColumn('trace_id', F.col('_value'))\
    .select('trace_id', 'EventString_value')\
    .groupBy('trace_id')\
    .agg(F.collect_list('EventString_value').alias('trace'))\

print('SCHEMA PULITO')
traceEventLog.printSchema()
traceEventLog.show()

# todo con traceEventLog posso procedere ad applicare l algoritmo BIG

df = df.withColumn('trace_id', F.col('string')._value) \
    .withColumn('trace', F.explode(F.col('event').string).alias('trace')) \
    .withColumn('trace', F.col('trace')[1]._value) \
    .groupBy('trace_id') \
    .agg(F.collect_list('trace').alias('trace'))

df.printSchema()

df = df.join(df_alignments_cr, df.trace_id == df_alignments_cr.alignments_cr_trace_id, 'inner') \
    .select('trace_id', 'trace', 'causal_relations', 'alignments') \
    .orderBy('trace_id')


# ----- FUNCTION USED IN irregularGraphRepairing  ----- #


def DeletionRepair(Wi, V, d_elements, cr):
    Wr1 = []
    Wr2 = []
    i = d_elements[0][0]
    if (d_elements[-1][1], V[i - 1][1]) in cr:
        for edge in Wi:
            if edge[1][0] == i and edge[0][0] < i:
                for h in range(edge[0][0], i):
                    if (V[h - 1][1], d_elements[0][1]) in cr:
                        Wr1.append(edge)
                        break
            if edge[0][0] < i < edge[1][0]:
                if (edge[0][1], d_elements[0][1]) in cr:
                    for l in range(i - 1, edge[1][0]):
                        if (V[l - 1], edge[1]) in Wi:
                            Wr2.append(edge)
                            break
    Wi = list(set(Wi) - set(Wr1) - set(Wr2))
    for k in range(i - 1, 0, -1):
        for j in range(i, len(V) + 1):
            if (V[k - 1][1], d_elements[0][1]) in cr:
                if (d_elements[-1][1], V[j - 1][1]) in cr:
                    if not isReachable(V, Wi, V[k - 1], V[j - 1]):
                        flag1 = True
                        for l in range(k - 1, j):
                            if (V[k - 1], V[l - 1]) in Wi:
                                flag1 = False
                                break
                        flag2 = True
                        for m in range(k - 1, i):
                            if (V[m - 1], V[j - 1]) in Wi:
                                flag2 = False
                                break
                        if flag1 or flag2:
                            Wi.append((V[k - 1], V[j - 1]))
    return Wi


def InsertionRepair(Wi, V, i_elements, cr):
    Wr1 = []
    Wr2 = []
    Wr3 = []
    Wr4 = []
    Wr5 = []
    Wa1 = []
    Wa2 = []
    Wa3 = []
    i = i_elements[0][0]
    j = i + len(i_elements) - 1
    only_labels = []
    for element in i_elements:
        if element[1] not in only_labels:
            only_labels.append(element[1])
    for edge in Wi:
        if edge[0][0] < i <= edge[1][0] <= j:
            Wr1.append(edge)
        if i <= edge[0][0] <= j < edge[1][0]:
            Wr2.append(edge)
        if i <= edge[0][0] <= j and i <= edge[1][0] <= j:
            Wr3.append(edge)
    Wi = list(set(Wi) - set(Wr1) - set(Wr2) - set(Wr3))
    for k in range(j + 1, len(V) + 1):
        if V[k - 1][1] not in only_labels:
            if (V[i - 2][1], V[k - 1][1]) in cr or (V[i - 2], V[k - 1]) in Wi:
                if not isReachable(V, Wi, V[j - 1], V[k - 1]):
                    Wi.append((V[j - 1], V[k - 1]))
                    Wa1.append((V[j - 1], V[k - 1]))
    if (V[i - 2][1], V[i][1]) not in cr:
        Wi.append((V[i - 2], V[i - 1]))
        Wa2.append((V[i - 2], V[i - 1]))
    else:
        for k in range(i - 1, 0, -1):
            if V[k - 1][1] not in only_labels:
                if (V[k - 1][1], V[j][1]) in cr or (V[k - 1], V[j]) in Wi:
                    if not isReachable(V, Wi, V[k - 1], V[i - 1]):
                        Wi.append((V[k - 1], V[i - 1]))
                        Wa2.append((V[k - 1], V[i - 1]))
    for k in range(len(i_elements) - 1):
        Wa3.append((V[k + i - 1], V[k + i]))
    if len(Wa3) > 0:
        for edge in Wa3:
            Wi.append(edge)
    for edge in Wa2:
        for edge2 in Wa1:
            if edge[1][0] >= i and edge[1][0] <= j:
                if edge2[0][0] >= i and edge2[0][0] <= j:
                    Wr4.append((edge[0], edge2[1]))
    Wi = list(set(Wi) - set(Wr4))
    if (V[i - 2][1], V[i][1]) not in cr:
        for edge in Wi:
            if edge[1][0] > i and edge[0][0] == i - 1:
                Wr5.append(edge)
                Wi = list(set(Wi) - set(Wr5))
    return Wi


def isReachable(V, W, s, d):
    # Mark all the vertices as not visited
    visited = [False] * (len(V))
    # Create a queue for BFS
    queue = []
    # Mark the source node as visited and enqueue it
    queue.append(s)
    visited[s[0] - 1] = True
    while queue:
        # Dequeue a vertex from queue
        j = queue.pop(0)
        # If this adjacent node is the destination node, then return true
        if j == d:
            return True
        # Else, continue to do BFS
        for edge in W:
            if edge[0] == j:
                if visited[edge[1][0] - 1] == False:
                    queue.append(edge[1])
                    visited[edge[1][0] - 1] = True
    # If BFS is complete without visited d
    return False


# ----- FUNCTIONS TO CONVERT IN UDF ----- #

def create_V(trace):
    event_id = 1
    V = []
    for event in trace:
        V.append((event_id, event))
        event_id += 1
    return V


def create_W(cr, V):
    W = []
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
    return W


def create_D(alignments):
    D = []
    id = 0
    temp_d = []
    prev = False
    curr = False
    deletion = False
    for edge in alignments:
        if edge[1] is None:
            continue
        if edge[0] == '>>':
            if prev:
                id -= 1
            deletion = True
            temp_d.append((id + 1, edge[1]))
            curr = True
        if edge[1] == '>>':
            if deletion:
                id -= 1
                deletion = False
            curr = True
        id += 1
        if (prev and not curr):
            if len(temp_d) > 0:
                D.append(temp_d)
            temp_d = []
        prev = curr
        curr = False
    if len(temp_d) > 0:
        D.append(temp_d)
    return D


def create_I(alignments):
    I = []
    id = 0
    temp_i = []
    prev = False
    curr = False
    deletion = False
    for edge in alignments:
        if edge[1] is None:
            continue
        if edge[0] == '>>':
            if prev:
                id -= 1
            deletion = True
            curr = True
        if edge[1] == '>>':
            if deletion:
                id -= 1
                deletion = False
            temp_i.append((id + 1, edge[0]))
            curr = True
        id += 1
        if (prev and not curr):
            if len(temp_i) > 0:
                I.append(temp_i)
            temp_i = []
        prev = curr
        curr = False
    if len(temp_i) > 0:
        I.append(temp_i)
    return I


def irregularGraphRepairing(V, W, D, I, cr):
    if len(D) + len(I) > 0:
        Wi = W
        for d_element in D:
            Wi = DeletionRepair(Wi, V, d_element, cr)
        for i_element in I:
            Wi = InsertionRepair(Wi, V, i_element, cr)
        return Wi


# --- SCHEMA FOR NODE COLUMN [ V ] --- #
V_schema = ArrayType(
    StructType(
        [
            StructField('node_number', IntegerType()),
            StructField('event', StringType()),
        ]
    )
)
# --- SCHEMA FOR EDGE COLUMN [ W ] and [ Wi ] --- #
W_schema = ArrayType(
    StructType(
        [
            StructField('start', StructType(
                [
                    StructField('start_arch_number', IntegerType()),
                    StructField('start_arch', StringType()),
                ]
            )),
            StructField('end', StructType(
                [
                    StructField('end_arch_number', IntegerType()),
                    StructField('end_arch', StringType()),
                ]
            )),
        ]
    )
)

# --- SCHEMA FOR DELETION AND INSERTION NODES [ D ] [ I ] --- #
D_I_schema = ArrayType(
    ArrayType(
        StructType(
            [
                StructField('repair_num', IntegerType()),
                StructField('repair_event', StringType()),
            ]
        )
    )
)

# --- UDF --- #
udf_create_V = F.udf(lambda trace: create_V(trace), V_schema)
udf_create_W = F.udf(lambda cr, V: create_W(cr, V), W_schema)
udf_create_D = F.udf(lambda alignments: create_D(alignments), D_I_schema)
udf_create_I = F.udf(lambda alignments: create_I(alignments), D_I_schema)
udf_irregularGraphRepairing = F.udf(lambda V, W, D, I, cr: irregularGraphRepairing(V, W, D, I, cr), W_schema)

################################################################################################################
final_df = df.withColumn('V', udf_create_V('trace')) \
    .withColumn('W', udf_create_W('causal_relations', 'V')) \
    .withColumn("D", udf_create_D('alignments')) \
    .withColumn("I", udf_create_I('alignments')) \
    .withColumn('Wi', udf_irregularGraphRepairing('V', 'W', 'D', 'I', 'causal_relations')) \
    .select('trace_id', 'V', 'W', 'D', 'I', 'Wi')

final_df.show()
dataCollect = final_df.collect()
end = time.time()
print("TEMPO IMPIEGATO: " + str((end - start)) + " secondi")

#
