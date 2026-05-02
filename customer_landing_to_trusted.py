import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import Filter
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read directly from S3
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://wgutdao20/customer/landing/"], "recurse": True},
    format="json",
    transformation_ctx="datasource"
)

# Convert to Spark DataFrame, filter, convert back
df = datasource.toDF()
df.createOrReplaceTempView("customer_landing")

filtered_df = spark.sql("""
    SELECT * FROM customer_landing
    WHERE shareWithResearchAsOfDate IS NOT NULL
    AND shareWithResearchAsOfDate <> 0
""")

print(f"Filtered row count: {filtered_df.count()}")

# Convert back to DynamicFrame
filtered_dynamic = DynamicFrame.fromDF(filtered_df, glueContext, "filtered_dynamic")

# Write to S3 and update Glue Data Catalog
sink = glueContext.getSink(
    path="s3://wgutdao20/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="",
    enableUpdateCatalog=True,
    transformation_ctx="sink"
)
sink.setCatalogInfo(catalogDatabase="stedi_db", catalogTableName="customer_trusted")
sink.setFormat("json")
sink.writeFrame(filtered_dynamic)

job.commit()