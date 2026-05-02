import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read step trainer landing and customer curated from S3
step_trainer_df = spark.read.json("s3://wgutdao20/step_trainer/landing/")
customer_curated_df = spark.read.json("s3://wgutdao20/customer/curated/")

step_trainer_df.createOrReplaceTempView("step_trainer_landing")
customer_curated_df.createOrReplaceTempView("customer_curated")

# Inner join on serial number — output only step trainer columns
filtered_df = spark.sql("""
    SELECT DISTINCT
        st.sensorReadingTime,
        st.serialNumber,
        st.distanceFromObject
    FROM step_trainer_landing st
    INNER JOIN customer_curated curated
    ON st.serialNumber = curated.serialNumber
""")

print(f"Step trainer trusted row count: {filtered_df.count()}")

filtered_df.write.mode("overwrite").json("s3://wgutdao20/step_trainer/trusted/")

job.commit()