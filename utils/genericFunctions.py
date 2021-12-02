import pyspark.sql.functions as F


def create_df_from_xes(spark_session, path):
    """
        Requires config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0") in spark config
        :param spark_session:
        :param path:
        :return: df:
    """

    df = spark_session.read.format('xml') \
        .option('rowTag', 'trace') \
        .option('valueTag', 'anyName') \
        .load(path)

    s = checkAttribute(df, 'string')

    if s:
        if isArray(df, 'string'):
            df = df.withColumn('String', F.explode(F.col('string')))
        else:
            df = df.withColumn('String', F.col('string'))

    df_1 = df

    if s:
        df = df.withColumn('traceString_key', F.col('String._key')) \
            .withColumn('traceString_value', F.col('String._value')) \
            .drop('String')

    ids = df.filter(df.traceString_key == 'concept:name').select('traceString_value')

    df_2 = df_1.withColumn('event', F.explode(F.col('event')).alias('event')) \
        .select('String._value', 'event')
    df_2 = df_2.join(ids, df_2._value == ids.traceString_value).select('_value', 'event')

    se = checkAttribute(df_2, 'event.string')

    if se:
        if isArray(df_2, 'event.string'):
            df_2 = df_2.withColumn('EventString', F.explode(F.col('event.string')))
        else:
            df_2 = df_2.withColumn('EventString', F.col('event.string'))
        df_2 = df_2.withColumn('EventString_key', F.col('EventString._key')) \
            .withColumn('EventString_value', F.col('EventString._value')) \
            .drop('EventString')

    df_3 = df_2.filter(df_2.EventString_key == 'concept:name') \
        .withColumn('trace_id', F.col('_value')) \
        .select('trace_id', 'EventString_value') \
        .groupBy('trace_id') \
        .agg(F.collect_list('EventString_value').alias('trace'))

    return df_3


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
