from udf import get_link, get_full_path
import spline_agent
from spline_agent.enums import WriteMode
from pyspark.sql.functions import col
import pyspark


@spline_agent.inputs("{datasets_table}", "{domains_table}")
def load(
    datasets_table: str,
    domains_table: str,
    spark: pyspark.sql.SparkSession,
    db_opts: dict,
) -> set:
    datasets_df = spark.read.jdbc(
        url=db_opts["url"], table=datasets_table, properties=db_opts["properties"]
    )
    domains_df = spark.read.jdbc(
        url=db_opts["url"], table=domains_table, properties=db_opts["properties"]
    )
    return datasets_df, domains_df


def join(
    datasets_df: pyspark.sql.DataFrame,
    domains_df: pyspark.sql.DataFrame,
    spark: pyspark.sql.SparkSession,
) -> pyspark.sql.DataFrame:
    return datasets_df.join(domains_df, "domain_id")


def get_additional_cols(joined_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    transformed_df = joined_df.withColumn(
        "domain_page", get_link(col("domain_name"))
    ).withColumn("full_path", get_full_path(col("domain_name"), col("file")))
    return transformed_df


@spline_agent.output("{result_table}", WriteMode.APPEND)
def dump(
    transformed_df: pyspark.sql.DataFrame, result_table: str, db_opts: dict
) -> None:
    transformed_df.write.jdbc(
        url=db_opts["url"],
        table=result_table,
        mode="overwrite",
        properties=db_opts["properties"],
    )
