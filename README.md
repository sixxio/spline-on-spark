# PySpark job with Spline Tracking

### Repo structure:  
|  
|- entrypoint.py   #  main func
|- dq.py           #  dq checks
|- etl.py          #  loading, joining and dumping
|- udf.pu          #  udf funcs


### Make sure you have env vars:
- JAVA_HOME
- SPARK_HOME
- HADOOP_HOME
- PYSPARK_PYTHON
- SPLINE_LINEAGE_DISPATCHER_TYPE
- SPLINE_LINEAGE_DISPATCHER_HTTP_BASE_URL
- SPARK_VERSION

### Make sure you have installed:
- pyspark
- spline-agent
- pydeequ
- postgresql driver for jdbc

### Pipeline description:
- tables domains and datasets are loaded from postregsql db
- tables are joined on domain_id column
- udf add full_path to dataset file (s3 URL) and domain_page URL
- pydeequ VerificationSuite checks for columns persistence and URL format
- joined and checked df loads as result into postgresql


Domains table:  
![image](https://github.com/sixxio/spline-on-spark/assets/91161909/63104a99-8544-4cda-b88a-b2538160c101)

Datasets table:  
![image](https://github.com/sixxio/spline-on-spark/assets/91161909/6366b371-9e3d-42a6-a064-2da4bf9a8653)

Result table:  
![image](https://github.com/sixxio/spline-on-spark/assets/91161909/d7b22608-0602-4596-a3f8-5be674b55e09)


Tracked data lineage:  
![image](https://github.com/sixxio/spline-on-spark/assets/91161909/0476a034-6d06-435c-8774-1a49a08cf959)

