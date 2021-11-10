# from pyspark.context import SparkContext
# from pyspark.sql.session import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pm4py.objects.log.importer.xes.variants import iterparse as xes_importer
from pm4py import conformance as conf
from pm4py.objects.petri_net.importer.variants import pnml as pnml_importer
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
import pm4py.algo.discovery.causal.variants.heuristic as cr_discovery
import pyspark.sql.functions as F
from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments

# sc = SparkContext.getOrCreate()
# spark = SparkSession(sc)
path_pnml = "/media/sf_cartella_condivisa/progetto/Big_pyspark/toyex_petriNet.pnml"
path_xes = "/media/sf_cartella_condivisa/progetto/Big_pyspark/toyex.xes"

# IMPORT PETRI NET
net, im, fm = pnml_importer.import_net(path_pnml)
# IMPORTA EVENTLOG DA FILE.XES
log = xes_importer.import_log(path_xes)

new_df = []
i = 0
# CONFORMANCE CHECKING
check = conf.conformance_diagnostics_token_based_replay(log, net, im, fm)
# DIRECT FOLLOW GRAPH FROM LOG
dfg = dfg_discovery.apply(log)

# extract a dictionary with all causal relation as key and a value (-1,1) indicating how strong is the relation
CR = cr_discovery.apply(dfg)
cr = []

# add in cr array all the relation with value grater than 0.8 (filter)
for key, val in CR.items():
    if val > 0.8:
        # to_write = '->'.join([key[0], key[1]])
        cr.append(key)

# popolare array per costruzione rdd -> dataframe
for trace in log:
    # if (check[i]['trace_is_fit']):
    aligned_traces = alignments.apply(trace, net, im, fm)['alignment']
    id_traccia = trace.attributes['concept:name']
    activities = []
    for event in trace:
        activities.append(event['concept:name'])
    new_df.append((id_traccia, activities, cr, aligned_traces))
    i += 1

schema = StructType([
    StructField('Trace_ID', StringType(), True),
    StructField('Trace', ArrayType(StringType()), True),
    StructField('Causal_relations', ArrayType(
        StructType(
            [
                StructField('cr_strart', StringType()),
                StructField('cr_end', StringType()),
            ]
        )
    ), True),
    StructField('Alignments', ArrayType(
        StructType(
            [
                StructField('on_log', StringType()),
                StructField('on_model', StringType()),
            ]
        )
    ), True),
])

rdd = spark.sparkContext.parallelize(new_df)
dataframe_fit = spark.createDataFrame(rdd, schema)


# funzione per estrazione nodi da traccia
def create_V(trace):
    event_id = 1
    V = []
    for event in trace:
        V.append((event_id, event))
        event_id += 1
    return V


# schema della colonna di output (V)
schema = ArrayType(
    StructType(
        [
            StructField('node_number', IntegerType()),
            StructField('event', StringType()),
        ]
    )
)

# udf per creazione colonna V (nodi)
udf_create_V = F.udf(lambda trace: create_V(trace), schema)


# funzione per estrazione archi da traccia
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


# schema della colonna di output (W)
schema = ArrayType(
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

# udf per creazione colonna W (archi)
udf_create_W = F.udf(lambda cr, V: create_W(cr, V), schema)


# funzione per confCheck Deletion
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


# schema della colonne di output (D e I)
schema = ArrayType(
    ArrayType(
        StructType(
            [
                StructField('repair_num', IntegerType()),
                StructField('repair_event', StringType()),
            ]
        )
    )
)

# udf per creazione colonne I
udf_create_D = F.udf(lambda alignments: create_D(alignments), schema)


# funzione per confCheck Insertion
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


# udf per creazione colonne I
udf_create_I = F.udf(lambda alignments: create_I(alignments), schema)

########################################################################################################

# TODO adattare le funzioni per usare dataframe

from IPython import display
from graphviz import Digraph


def viewInstanceGraph(V, W, title="Instance Graph"):
    # Conversion to string indexes
    V2 = []
    W2 = []
    for node in V:
        V2.append((str(node[0]), node[1]))
    for edge in W:
        W2.append(((str(edge[0][0]), edge[0][1]), (str(edge[1][0]), edge[1][1])))

    dot = Digraph(comment=title, node_attr={'shape': 'circle'})
    for e in V2:
        dot.node(e[0], e[1])
    for w in W2:
        dot.edge(w[0][0], w[1][0])
    display.display(dot)


def isTraceMatching(V, trace):
    trace = trace.split(',')
    flag = True
    for i in range(len(V)):
        if trace[i] != V[i][1]:
            flag = False
            break
    return flag


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

            if edge[0][0] < i and edge[1][0] > i:
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
        if edge[0][0] < i and edge[1][0] >= i and edge[1][0] <= j:
            Wr1.append(edge)
        if edge[0][0] >= i and edge[0][0] <= j and edge[1][0] > j:
            Wr2.append(edge)
        if edge[0][0] >= i and edge[0][0] <= j and edge[1][0] >= i and edge[1][0] <= j:
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


def irregularGraphReparing(V, W, D, I, cr):
    Wi = W
    for d_element in D:
        Wi = DeletionRepair(Wi, V, d_element, cr)
        print("Deletion repaired Instance Graph")
        viewInstanceGraph(V, Wi)
    for i_element in I:
        Wi = InsertionRepair(Wi, V, i_element, cr)
        print("Insertion repaired Instance Graph")
        viewInstanceGraph(V, Wi)
    return Wi


# TODO adattare le funzioni fino a qui!!!

# TODO man mano che vengono adattate le funzioni replica main in file tmp.py
################################################################################
df = dataframe_fit.withColumn('V', udf_create_V('Trace')) \
    .withColumn('W', udf_create_W('Causal_relations', 'V')) \
    .withColumn("D", udf_create_D('Alignments')) \
    .withColumn("I", udf_create_I('Alignments')) \
    .select('Trace_ID', 'V', 'W', 'D', 'I')

df.show(5)
################################################################################
