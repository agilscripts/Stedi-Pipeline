import sys
from awsglue.transforms import Join
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read trusted accelerometer data
accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted"
)

# Read trusted step trainer data
step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted"
)

# Join on matching timestamps (adjust keys if needed)
ml_curated = Join.apply(
    frame1=accelerometer_trusted,
    frame2=step_trainer_trusted,
    keys1=["timestamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="ml_curated"
)

# Save the aggregated ML data to S3
output_path = "s3://stedi-project/machine_learning_curated/"
glueContext.write_dynamic_frame.from_options(
    frame=ml_curated,
    connection_type="s3",
    connection_options={"path": output_path},
    format="json",
    transformation_ctx="datasink"
)

job.commit()
