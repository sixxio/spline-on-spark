from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def get_full_path(domain_name, file_name):
    return (
        f"s3://{domain_name}/{file_name}"
        if (domain_name is not None and file_name is not None)
        else None
    )


@udf(returnType=StringType())
def get_link(domain_name):
    return f"https://sample.com/{domain_name}" if domain_name is not None else None
