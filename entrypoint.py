import os

os.environ["SPARK_VERSION"] = "3.1"
import pydeequ

os.environ["SPLINE_LINEAGE_DISPATCHER_TYPE"] = "http"
os.environ["SPLINE_LINEAGE_DISPATCHER_HTTP_BASE_URL"] = (
    "http://192.168.3.3:7070/producer"
)
import spline_agent

from pyspark.sql import SparkSession
from dq import validate
from etl import load, join, get_additional_cols, dump


@spline_agent.track_lineage()
def etl(
    datasets_table: str, domains_table: str, result_table: str, db_opts: dict
) -> None:

    spark = (
        SparkSession.builder.appName("Datasets and domains join.")
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
    )
    datasets_df, domains_df = load(datasets_table, domains_table, spark, db_opts)
    joined_df = join(datasets_df, domains_df, spark)
    transformed_df = get_additional_cols(joined_df)
    if validate(spark, transformed_df):
        dump(transformed_df, result_table, db_opts)

    spark.stop()


etl(
    datasets_table="datasets",
    domains_table="domains",
    result_table="result",
    db_opts={
        "url": "jdbc:postgresql://192.168.3.3:5001/main",
        "properties": {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver",
        },
    },
)
