from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as F



print('NUMERO DI TRACCE:\n' + str(ids.count()))

# todo con traceEventLog posso procedere ad applicare l algoritmo BIG

# df = traceEventLog.join(df_alignments_cr, traceEventLog.trace_id == df_alignments_cr.alignments_cr_trace_id, 'inner') \
#     .select('trace_id', 'trace', 'causal_relations', 'alignments')

# ----- FUNCTION USED IN irregularGraphRepairing  ----- #


# def DeletionRepair(Wi, V, d_elements, cr):
#     Wr1 = []
#     Wr2 = []
#     i = d_elements[0][0]
#     if (d_elements[-1][1], V[i - 1][1]) in cr:
#         for edge in Wi:
#             if edge[1][0] == i and edge[0][0] < i:
#                 for h in range(edge[0][0], i):
#                     if (V[h - 1][1], d_elements[0][1]) in cr:
#                         Wr1.append(edge)
#                         break
#             if edge[0][0] < i < edge[1][0]:
#                 if (edge[0][1], d_elements[0][1]) in cr:
#                     for l in range(i - 1, edge[1][0]):
#                         if (V[l - 1], edge[1]) in Wi:
#                             Wr2.append(edge)
#                             break
#     Wi = list(set(Wi) - set(Wr1) - set(Wr2))
#     for k in range(i - 1, 0, -1):
#         for j in range(i, len(V) + 1):
#             if (V[k - 1][1], d_elements[0][1]) in cr:
#                 if (d_elements[-1][1], V[j - 1][1]) in cr:
#                     if not isReachable(V, Wi, V[k - 1], V[j - 1]):
#                         flag1 = True
#                         for l in range(k - 1, j):
#                             if (V[k - 1], V[l - 1]) in Wi:
#                                 flag1 = False
#                                 break
#                         flag2 = True
#                         for m in range(k - 1, i):
#                             if (V[m - 1], V[j - 1]) in Wi:
#                                 flag2 = False
#                                 break
#                         if flag1 or flag2:
#                             Wi.append((V[k - 1], V[j - 1]))
#     return Wi
#
#
# def InsertionRepair(Wi, V, i_elements, cr):
#     Wr1 = []
#     Wr2 = []
#     Wr3 = []
#     Wr4 = []
#     Wr5 = []
#     Wa1 = []
#     Wa2 = []
#     Wa3 = []
#     i = i_elements[0][0]
#     j = i + len(i_elements) - 1
#     only_labels = []
#     for element in i_elements:
#         if element[1] not in only_labels:
#             only_labels.append(element[1])
#     for edge in Wi:
#         if edge[0][0] < i <= edge[1][0] <= j:
#             Wr1.append(edge)
#         if i <= edge[0][0] <= j < edge[1][0]:
#             Wr2.append(edge)
#         if i <= edge[0][0] <= j and i <= edge[1][0] <= j:
#             Wr3.append(edge)
#     Wi = list(set(Wi) - set(Wr1) - set(Wr2) - set(Wr3))
#     for k in range(j + 1, len(V) + 1):
#         if V[k - 1][1] not in only_labels:
#             if (V[i - 2][1], V[k - 1][1]) in cr or (V[i - 2], V[k - 1]) in Wi:
#                 if not isReachable(V, Wi, V[j - 1], V[k - 1]):
#                     Wi.append((V[j - 1], V[k - 1]))
#                     Wa1.append((V[j - 1], V[k - 1]))
#     if (V[i - 2][1], V[i][1]) not in cr:
#         Wi.append((V[i - 2], V[i - 1]))
#         Wa2.append((V[i - 2], V[i - 1]))
#     else:
#         for k in range(i - 1, 0, -1):
#             if V[k - 1][1] not in only_labels:
#                 if (V[k - 1][1], V[j][1]) in cr or (V[k - 1], V[j]) in Wi:
#                     if not isReachable(V, Wi, V[k - 1], V[i - 1]):
#                         Wi.append((V[k - 1], V[i - 1]))
#                         Wa2.append((V[k - 1], V[i - 1]))
#     for k in range(len(i_elements) - 1):
#         Wa3.append((V[k + i - 1], V[k + i]))
#     if len(Wa3) > 0:
#         for edge in Wa3:
#             Wi.append(edge)
#     for edge in Wa2:
#         for edge2 in Wa1:
#             if edge[1][0] >= i and edge[1][0] <= j:
#                 if edge2[0][0] >= i and edge2[0][0] <= j:
#                     Wr4.append((edge[0], edge2[1]))
#     Wi = list(set(Wi) - set(Wr4))
#     if (V[i - 2][1], V[i][1]) not in cr:
#         for edge in Wi:
#             if edge[1][0] > i and edge[0][0] == i - 1:
#                 Wr5.append(edge)
#                 Wi = list(set(Wi) - set(Wr5))
#     return Wi
#
#
# def isReachable(V, W, s, d):
#     # Mark all the vertices as not visited
#     visited = [False] * (len(V))
#     # Create a queue for BFS
#     queue = []
#     # Mark the source node as visited and enqueue it
#     queue.append(s)
#     visited[s[0] - 1] = True
#     while queue:
#         # Dequeue a vertex from queue
#         j = queue.pop(0)
#         # If this adjacent node is the destination node, then return true
#         if j == d:
#             return True
#         # Else, continue to do BFS
#         for edge in W:
#             if edge[0] == j:
#                 if visited[edge[1][0] - 1] == False:
#                     queue.append(edge[1])
#                     visited[edge[1][0] - 1] = True
#     # If BFS is complete without visited d
#     return False
#
#
# # ----- FUNCTIONS TO CONVERT IN UDF ----- #
#
# def create_V(trace):
#     event_id = 1
#     V = []
#     for event in trace:
#         V.append((event_id, event))
#         event_id += 1
#     return V
#
#
# def create_W(cr, V):
#     W = []
#     for i in range(len(V)):
#         for k in range(i, len(V)):
#             e1 = V[i]
#             e2 = V[k]
#             if (e1[1], e2[1]) in cr:
#                 flag_e1 = True
#                 for s in range(i + 1, k):
#                     e3 = V[s]
#                     if (e1[1], e3[1]) in cr:
#                         flag_e1 = False
#                 flag_e2 = True
#                 for s in range(i + 1, k):
#                     e3 = V[s]
#                     if (e3[1], e2[1]) in cr:
#                         flag_e2 = False
#                 if flag_e1 or flag_e2:
#                     W.append((e1, e2))
#     return W
#
#
# def create_D(alignments):
#     D = []
#     id = 0
#     temp_d = []
#     prev = False
#     curr = False
#     deletion = False
#     for edge in alignments:
#         if edge[1] is None:
#             continue
#         if edge[0] == '>>':
#             if prev:
#                 id -= 1
#             deletion = True
#             temp_d.append((id + 1, edge[1]))
#             curr = True
#         if edge[1] == '>>':
#             if deletion:
#                 id -= 1
#                 deletion = False
#             curr = True
#         id += 1
#         if (prev and not curr):
#             if len(temp_d) > 0:
#                 D.append(temp_d)
#             temp_d = []
#         prev = curr
#         curr = False
#     if len(temp_d) > 0:
#         D.append(temp_d)
#     return D
#
#
# def create_I(alignments):
#     I = []
#     id = 0
#     temp_i = []
#     prev = False
#     curr = False
#     deletion = False
#     for edge in alignments:
#         if edge[1] is None:
#             continue
#         if edge[0] == '>>':
#             if prev:
#                 id -= 1
#             deletion = True
#             curr = True
#         if edge[1] == '>>':
#             if deletion:
#                 id -= 1
#                 deletion = False
#             temp_i.append((id + 1, edge[0]))
#             curr = True
#         id += 1
#         if (prev and not curr):
#             if len(temp_i) > 0:
#                 I.append(temp_i)
#             temp_i = []
#         prev = curr
#         curr = False
#     if len(temp_i) > 0:
#         I.append(temp_i)
#     return I
#
#
# def irregularGraphRepairing(V, W, D, I, cr):
#     if len(D) + len(I) > 0:
#         Wi = W
#         for d_element in D:
#             Wi = DeletionRepair(Wi, V, d_element, cr)
#         for i_element in I:
#             Wi = InsertionRepair(Wi, V, i_element, cr)
#         return Wi
#
#
# # --- SCHEMA FOR NODE COLUMN [ V ] --- #
# V_schema = ArrayType(
#     StructType(
#         [
#             StructField('node_number', IntegerType()),
#             StructField('event', StringType()),
#         ]
#     )
# )
# # --- SCHEMA FOR EDGE COLUMN [ W ] and [ Wi ] --- #
# W_schema = ArrayType(
#     StructType(
#         [
#             StructField('start', StructType(
#                 [
#                     StructField('start_arch_number', IntegerType()),
#                     StructField('start_arch', StringType()),
#                 ]
#             )),
#             StructField('end', StructType(
#                 [
#                     StructField('end_arch_number', IntegerType()),
#                     StructField('end_arch', StringType()),
#                 ]
#             )),
#         ]
#     )
# )
#
# # --- SCHEMA FOR DELETION AND INSERTION NODES [ D ] [ I ] --- #
# D_I_schema = ArrayType(
#     ArrayType(
#         StructType(
#             [
#                 StructField('repair_num', IntegerType()),
#                 StructField('repair_event', StringType()),
#             ]
#         )
#     )
# )
#
# # --- UDF --- #
# udf_create_V = F.udf(lambda trace: create_V(trace), V_schema)
# udf_create_W = F.udf(lambda cr, V: create_W(cr, V), W_schema)
# udf_create_D = F.udf(lambda alignments: create_D(alignments), D_I_schema)
# udf_create_I = F.udf(lambda alignments: create_I(alignments), D_I_schema)
# udf_irregularGraphRepairing = F.udf(lambda V, W, D, I, cr: irregularGraphRepairing(V, W, D, I, cr), W_schema)
#
# ################################################################################################################
# final_df = df.withColumn('V', udf_create_V('trace')) \
#     .withColumn('W', udf_create_W('causal_relations', 'V')) \
#     .withColumn("D", udf_create_D('alignments')) \
#     .withColumn("I", udf_create_I('alignments')) \
#     .withColumn('Wi', udf_irregularGraphRepairing('V', 'W', 'D', 'I', 'causal_relations')) \
#     .select('trace_id', 'V', 'W', 'D', 'I', 'Wi')
#
# final_df.show()
# dataCollect = final_df.collect()
# end = time.time()
# print("TEMPO IMPIEGATO: " + str((end - start)) + " secondi")
#
# #
