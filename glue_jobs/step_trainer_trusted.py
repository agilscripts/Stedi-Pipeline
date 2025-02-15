import sys
from awsglue.transforms import Join, SelectFields
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

# Read step trainer landing data
step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing"
)

# Read curated customer data (from a later job)
customer_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customer_curated"
)

# Join on "serialNumber" to filter step trainer records for curated customers
joined = Join.apply(
    frame1=step_trainer_landing,
    frame2=customer_curated,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="joined"
)

# Optionally, select only step trainer fields
selected = joined.select_fields(["sensorReadingTime", "serialNumber", "distanceFromObject"])

# Save the trusted step trainer records to S3
output_path = "s3://stedi-project/step_trainer_trusted/"
glueContext.write_dynamic_frame.from_options(
    frame=selected,
    connection_type="s3",
    connection_options={"path": output_path},
    format="json",
    transformation_ctx="datasink"
)

job.commit()
