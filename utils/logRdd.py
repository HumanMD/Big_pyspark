from pm4py.objects.log.obj import Trace, Event
from pm4py.util.dt_parsing import parser as dt_parser

from pyspark import Row


def create_rdd_from_xes(spark_session, path):
    """
    Requires config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0") in spark config
    :param spark_session:
    :param path:
    :return: rdd:
    """
    df = spark_session.read \
        .format("com.databricks.spark.xml") \
        .option("rootTag", "log") \
        .option("rowTag", "trace") \
        .option('valueTag', 'anyName') \
        .option("inferSchema", "false") \
        .load(path)

    return df.rdd.map(lambda r: parse_xml_row(r))


def parse_xml_row(row: Row):
    # it must contain at least one field called event, containing a list of elements under the parent tag
    # <event></event>
    events = [xml_row_to_dict(r) for r in row["event"]]
    # the rest of the row represent the information on the trace
    r_dict = row.asDict()
    trace_info_raw = [r_dict[i] for i in r_dict.keys() if i != "event"]
    trace_info = xml_row_to_dict(trace_info_raw)
    return from_dicts_to_trace(events, trace_info)


def xml_row_to_dict(row):
    d = {}
    # first level groups attributes by tag name. For example, if there is one attribute in a <date> tag,
    # and two attributes in two <string> tags, then there will be one element inside the "date" row attribute,
    # and a list with two elements inside the "string" row attribute
    for element in row:
        # if there is more than one element witht his type of tag
        if type(element) == list:
            for att in element:
                d.update(extract_key_and_value_from_xml(att.asDict()))
        else:
            d.update(extract_key_and_value_from_xml(element.asDict()))
    return d


def extract_key_and_value_from_xml(d):
    if "_key" in d.keys():
        if "_value" in d.keys():
            return {d["_key"]: d["_value"]}
        else:
            return {d["_key"]: None}


def from_dicts_to_trace(event_dicts, trace_info_dict):
    events = [from_dict_to_event(ed) for ed in event_dicts]
    return Trace(events, attributes=trace_info_dict)


def from_dict_to_event(event_dict):
    timestamp_field_name = "time:timestamp"
    if timestamp_field_name in event_dict.keys():
        event_dict[timestamp_field_name] = dt_parser.get().apply(event_dict[timestamp_field_name])
    return Event(event_dict)
