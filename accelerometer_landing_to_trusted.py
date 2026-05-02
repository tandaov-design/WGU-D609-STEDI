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

accelerometer_df = spark.read.json("s3://wgutdao20/accelerometer/landing/")
customer_df = spark.read.json("s3://wgutdao20/customer/trusted/")

accelerometer_df.createOrReplaceTempView("accelerometer_landing")
customer_df.createOrReplaceTempView("customer_trusted")

filtered_df = spark.sql("""
    SELECT accel.user, accel.timeStamp, accel.x, accel.y, accel.z
    FROM accelerometer_landing accel
    INNER JOIN customer_trusted cust
    ON accel.user = cust.email
""")

print(f"Filtered accelerometer row count: {filtered_df.count()}")

filtered_dynamic = DynamicFrame.fromDF(filtered_df, glueContext, "filtered_dynamic")

sink = glueContext.getSink(
    path="s3://wgutdao20/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="",
    enableUpdateCatalog=True,
    transformation_ctx="sink"
)
sink.setCatalogInfo(catalogDatabase="stedi_db", catalogTableName="accelerometer_trusted")
sink.setFormat("json")
sink.writeFrame(filtered_dynamic)

job.commit()