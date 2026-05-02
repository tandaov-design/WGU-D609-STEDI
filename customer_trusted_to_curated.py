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

# Read customer trusted and accelerometer trusted from S3
customer_df = spark.read.json("s3://wgutdao20/customer/trusted/")
accelerometer_df = spark.read.json("s3://wgutdao20/accelerometer/trusted/")

customer_df.createOrReplaceTempView("customer_trusted")
accelerometer_df.createOrReplaceTempView("accelerometer_trusted")

# Inner join on email — output only customer columns, deduplicated
filtered_df = spark.sql("""
    SELECT DISTINCT
        cust.customerName,
        cust.email,
        cust.phone,
        cust.birthDay,
        cust.serialNumber,
        cust.registrationDate,
        cust.lastUpdateDate,
        cust.shareWithResearchAsOfDate,
        cust.shareWithPublicAsOfDate,
        cust.shareWithFriendsAsOfDate
    FROM customer_trusted cust
    INNER JOIN accelerometer_trusted accel
    ON cust.email = accel.user
""")

print(f"Customer curated row count: {filtered_df.count()}")

filtered_df.write.mode("overwrite").json("s3://wgutdao20/customer/curated/")

job.commit()