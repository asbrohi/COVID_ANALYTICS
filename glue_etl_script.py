import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read Parquet files from S3
s3_path = "s3://snowflakecoviddata-asb/transformed_covid/"
df = spark.read.option("mergeSchema", "true").parquet(s3_path)

# Optional Data Transformation (e.g., renaming columns or filtering)
df = df.withColumnRenamed("state", "State") \
       .withColumnRenamed("year", "Year") \
       .withColumnRenamed("month", "Month") \
       .withColumnRenamed("total_indian", "Total_Indian") \
       .withColumnRenamed("total_foreigners", "Total_Foreigners") \
       .withColumnRenamed("total_cured", "Total_Cured") \
       .withColumnRenamed("total_deaths", "Total_Deaths")

# Write to Snowflake
df.write.format("snowflake") \
    .option("sfURL", "your_snowflake_account.snowflakecomputing.com") \
    .option("sfDatabase", "ETL_PROJECT") \
    .option("sfSchema", "PUBLIC") \
    .option("sfWarehouse", "your_warehouse") \
    .option("sfRole", "your_role") \
    .option("sfUsername", "your_snowflake_username") \
    .option("sfPassword", "your_snowflake_password") \
    .option("sfTable", "COVID_ANALYTICS") \
    .mode("append") \
    .save()

# Commit the job
job.commit()