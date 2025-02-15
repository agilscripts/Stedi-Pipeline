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

# Read accelerometer landing data
accelerometer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing"
)

# Read trusted customer data
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted"
)

# Join the two datasets using the "email" field
joined = Join.apply(
    frame1=accelerometer_landing,
    frame2=customer_trusted,
    keys1=["email"],
    keys2=["email"],
    transformation_ctx="joined"
)

# Select only the accelerometer columns (adjust if needed)
selected = joined.select_fields(["serialNumber", "timestamp", "x", "y", "z", "email"])

# Save the trusted accelerometer records to S3
output_path = "s3://stedi-project/accelerometer_trusted/"
glueContext.write_dynamic_frame.from_options(
    frame=selected,
    connection_type="s3",
    connection_options={"path": output_path},
    format="json",
    transformation_ctx="datasink"
)

job.commit()
