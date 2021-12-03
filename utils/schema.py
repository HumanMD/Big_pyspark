from pyspark.sql.types import *

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