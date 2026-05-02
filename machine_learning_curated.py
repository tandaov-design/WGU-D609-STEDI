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

# Read step trainer trusted and accelerometer trusted from S3
step_trainer_df = spark.read.json("s3://wgutdao20/step_trainer/trusted/")
accelerometer_df = spark.read.json("s3://wgutdao20/accelerometer/trusted/")

step_trainer_df.createOrReplaceTempView("step_trainer_trusted")
accelerometer_df.createOrReplaceTempView("accelerometer_trusted")

# Inner join on timestamp
ml_df = spark.sql("""
    SELECT
        st.sensorReadingTime,
        st.serialNumber,
        st.distanceFromObject,
        accel.user,
        accel.x,
        accel.y,
        accel.z
    FROM step_trainer_trusted st
    INNER JOIN accelerometer_trusted accel
    ON st.sensorReadingTime = accel.timeStamp
""")

print(f"Machine learning curated row count: {ml_df.count()}")

ml_df.write.mode("overwrite").json("s3://wgutdao20/machine_learning/curated/")

job.commit()