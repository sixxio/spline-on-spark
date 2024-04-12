import os

os.environ["SPARK_VERSION"] = "3.1"

import pydeequ
from pydeequ.checks import *
from pydeequ.verification import *
import pyspark


def validate(
    spark: pyspark.sql.SparkSession, transformed_df: pyspark.sql.DataFrame
) -> bool:
    check = Check(spark, CheckLevel.Warning, "Review Check")
    checkResult = (
        VerificationSuite(spark)
        .onData(transformed_df)
        .addCheck(
            check.hasSize(lambda x: x >= 3)
            .areComplete(transformed_df.columns)
            .hasPattern("domain_page", r"https:\/\/sample\.com\/.*")
            .hasPattern("full_path", r"s3:\/\/.*\/.*")
            .isNonNegative("domain_id")
            .isUnique("dataset_id")
        )
        .run()
    )
    return checkResult.status == "Success"
