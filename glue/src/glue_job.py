# This job is processing acccess logs in JSON
# The job can take arguments for COLLECTOR_DATE and COLLECTOR_HOUR or use default LAST HOUR UTC.

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, ArrayType
from pyspark.sql.functions import col, when, lit, date_trunc, input_file_name, regexp_extract, concat_ws
from datetime import datetime, timedelta
from functools import reduce
from pyspark.sql import DataFrame

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)



# Define list of mandatory and optional job's arguments
mandatory_fields = [
    "JOB_NAME",
    "source_s3_bucket",
    "target_s3_bucket",
    "target_s3_prefix",
]
default_optional_args = {
    "collector_date": datetime.strftime(datetime.utcnow(), "%Y%m%d"),
    "collector_hour": datetime.strftime(datetime.utcnow() - timedelta(hours=1), "%H")
}


def get_glue_args(mandatory_fields, default_optional_args):
    """
    This is a wrapper of the glue function getResolvedOptions to take care of the following case :
    * Handling optional arguments and/or mandatory arguments
    * Optional arguments with default value
    NOTE:
        * DO NOT USE '-' while defining args as the getResolvedOptions with replace them with '_'
        * All fields would be return as a string type with getResolvedOptions

    Arguments:
        mandatory_fields {list} -- list of mandatory fields for the job
        default_optional_args {dict} -- dict for optional fields with their default value

    Returns:
        dict -- given args with default value of optional args not filled
    """
    # The glue args are available in sys.argv with an extra '--'
    given_optional_fields_key = list(
        set([i[2:] for i in sys.argv]).intersection([i for i in default_optional_args])
    )

    args = getResolvedOptions(sys.argv, mandatory_fields + given_optional_fields_key)

    # Overwrite default value if optional args are provided
    default_optional_args.update(args)

    return default_optional_args


args = get_glue_args(mandatory_fields, default_optional_args)

job_name = args["JOB_NAME"]
source_s3_bucket = args["source_s3_bucket"]
target_s3_bucket = args["target_s3_bucket"]
target_s3_prefix = args["target_s3_prefix"]
collector_date = args["collector_date"]
collector_hour = args["collector_hour"]

# dynamic partitions
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# timer start
start = datetime.now()

# schema 
schema = StructType([
    StructField("app_logs_meta", StructType([  
# ...
StructField("source_name", StringType(), True),

StructField("timestamp", StringType(), True),
])

# Union environments together
envs = [...]
first_df = True
dataframes = []

for env in envs:
    if env in [.....]:
        file_path = f"s3://{source_s3_bucket}/topics/applogs.access.{env}.safe/{collector_date}/{collector_hour}/"
        print(file_path)
    elif env in [..]:
        file_path = f"s3://{source_s3_bucket}/raw/access.event/environment={env}/collector_day={collector_date}/collector_hour={collector_hour}/"
        print(file_path)
    else:
        continue

    df = spark.read.json(file_path, schema = schema).withColumn("env",lit(env))
    dataframes.append(df)
    
    consolidated_df = reduce(DataFrame.unionByName, dataframes)
    
# transform
transformed_df = (consolidated_df
    .filter(col("timestamp").isNotNull()) 
    .filter(col("app_logs_meta.id").isNotNull())
    # take the columns from JSON logs
    # .withColumn(...)
    .withColumn("source_name", col("source_name"))
    .withColumn("timestamp", col("timestamp"))
    .withColumn("date_hour", date_trunc("hour", col("timestamp")))
   .drop("app_logs_meta","contents")
)

# write to Parquet
table_output = "s3a://datalake/applogs-glue/stg_access_logs/"
transformed_df.coalesce(100).write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("collector_hour") \
    .save(table_output)
job.commit()

stop = datetime.now()

print(f"Done with job {job_name} after: {stop - start}")
