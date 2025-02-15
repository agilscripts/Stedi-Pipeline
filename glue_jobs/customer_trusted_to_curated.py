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

# Read trusted customer data
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted"
)

# Read trusted accelerometer data
accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted"
)

# Join on "email" to get only customers with accelerometer data
curated = Join.apply(
    frame1=customer_trusted,
    frame2=accelerometer_trusted,
    keys1=["email"],
    keys2=["email"],
    transformation_ctx="curated"
)

# Keep only the customer columns
customer_curated = curated.select_fields([
    "serialNumber", "customerName", "email", "phone", "birthDay",
    "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate",
    "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"
])

# Save the curated customer data to S3
output_path = "s3://stedi-project/customer_curated/"
glueContext.write_dynamic_frame.from_options(
    frame=customer_curated,
    connection_type="s3",
    connection_options={"path": output_path},
    format="json",
    transformation_ctx="datasink"
)

job.commit()
