from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

# todo  posso procedere ad applicare l algoritmo BIG

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
