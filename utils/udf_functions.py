import pyspark.sql.functions as F

from utils.genericFunctions import create_V, create_W, create_D, create_I, irregularGraphRepairing
from utils.schema import V_schema, W_schema, D_I_schema

from pm4py.objects.log.importer.xes.variants import iterparse as xes_importer
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
import pm4py.algo.discovery.causal.variants.heuristic as cr_discovery
from main import rootLocalPathXes, xes, test

# CAUSAL RELATIONS (cr)
log = xes_importer.import_log(rootLocalPathXes + xes[test])
dfg = dfg_discovery.apply(log)
CR = cr_discovery.apply(dfg)
cr = [key for key, val in CR.items() if val > 0.8]

udf_create_V = F.udf(lambda trace: create_V(trace), V_schema)
udf_create_W = F.udf(lambda V: create_W(cr, V), W_schema)
udf_create_D = F.udf(lambda alignments: create_D(alignments), D_I_schema)
udf_create_I = F.udf(lambda alignments: create_I(alignments), D_I_schema)
udf_irregularGraphRepairing = F.udf(lambda V, W, D, I: irregularGraphRepairing(V, W, D, I, cr), W_schema)
