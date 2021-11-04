# from pyspark.context import SparkContext
# from pyspark.sql.session import SparkSession
from pyspark.sql import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DateType
import time as tempo
from pm4py.objects.log.importer.xes.variants import iterparse as xes_importer
from pm4py import conformance as conf
from pm4py.objects.petri_net.importer.variants import pnml as pnml_importer

# sc = SparkContext.getOrCreate()
# spark = SparkSession(sc)
path_pnml = "/media/sf_cartella_condivisa/progetto/BIG_Datasets/toyex_petriNet.pnml"
path_xes = "/media/sf_cartella_condivisa/progetto/BIG_Datasets/toyex.xes"
tempo_inizio = tempo.time()

net, im, fm = pnml_importer.import_net(path_pnml)
labels = []  # etichette valide
archi_head = []
archi_tail = []

for arco in net.arcs:
    stringa = str(arco)
    stringa = stringa.split('->')
    archi_head.append(stringa[0])
    archi_tail.append(stringa[1])
    if (stringa[0].find('(t)n') != 0 and stringa[0].find('(p)') != 0 and stringa[0] not in labels):
        labels.append(stringa[0])

parall_in = []
parall_out = []

# trova transizioni di input e output dei parallelismi, cioè da dove iniziano e finiscono
for a in archi_head:
    if (a.find('(t)') == 0 and archi_head.count(a) > 1 and a not in parall_in):
        parall_in.append(a)

for b in archi_tail:
    if (b.find('(t)') == 0 and archi_tail.count(b) > 1 and b not in parall_out):
        parall_out.append(b)

paral_labels = []
relations_tot = []
indice = 0

# ricerca di tutte le relazioni causali partendo dall'indice di un etichetta
def search(indice):
    head = archi_head[indice]
    tail = archi_tail[indice]
    if (tail in labels):
        relations.append(tail.replace('(t)', ''))
        return True
    elif (tail == out):
        return False
    considerate.append(head)
    if (tail in considerate):
        return False
    if (tail in archi_head):
        z = archi_head.count(tail)
        scroll = [pippo for pippo, val in enumerate(archi_head) if val == tail]
        for c in scroll:
            search(c)
    else:
        return False

# per trovare le relazioni causali di tutti i parallelismi identificati sopra
for inp in parall_in:
    uno = inp.replace('(t)', '')
    relations = []
    considerate = []
    out = parall_out[indice]
    scroll = [pippo for pippo, val in enumerate(archi_head) if val == inp]
    for e in scroll:
        search(e)
    paral_labels.append(relations)
    for e in relations:
        coppia = [uno, e]
        if (coppia not in relations_tot):
            relations_tot.append(coppia)
    indice += 1

out = ''
considerate = []
relations = []

# cerca le relazioni causali per ogni attività valida esplorando tutti i percorsi nella rete di petri
for start in labels:
    search(archi_head.index(start))
    start = start.replace('(t)', '')
    for x in relations:
        coppia = [start, x]
        if (coppia not in relations_tot):
            relations_tot.append(coppia)
    considerate.clear()
    relations.clear()

print("\n ***PARALLELISMI TROVATI***")
for x in paral_labels:
    print(x)

# IMPORTA EVENTLOG DA FILE.XES
log = xes_importer.import_log(path_xes)

check = conf.conformance_tbr(log, net, im, fm)

print("\nTempo impiegato per trovare tutte le relazioni causali ed i parallelismi")
print(tempo.time() - tempo_inizio)
print("\n Numero di tracce totali nel file: " + str(len(log)))

new_df = []
i = 0

# popolare array per costruzione rdd -> dataframe
for trace in log:
    if (check[i]['trace_is_fit']):
        id_traccia = trace.attributes['concept:name']
        for event in trace:
            activity = event['concept:name']
            time = event['time:timestamp']
            new_df.append((id_traccia, activity, time))
    i += 1

print("\nTempo per conf check e prendere le tracce che fittano:")
print(tempo.time() - tempo_inizio)

schema = StructType([
    StructField('Trace', StringType(), True),
    StructField('Activity', StringType(), True),
    StructField('Time', DateType(), True)
])

rdd = spark.sparkContext.parallelize(new_df)
dataframe_fit = spark.createDataFrame(rdd, schema)

id_tracce = dataframe_fit.select("Trace").distinct().collect()
tot_tracce = len(id_tracce)
print("\n Numero di tracce che hanno fittato: " + str(tot_tracce))
