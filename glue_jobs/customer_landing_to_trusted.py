import sys
from awsglue.transforms import Filter
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

# Read customer landing data from the Data Catalog
customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="customer_landing"
)

# Keep only records with a valid shareWithResearchAsOfDate
customer_trusted = Filter.apply(
    frame=customer_landing,
    f=lambda row: row.get("shareWithResearchAsOfDate") not in (None, "", "null")
)

# Save the trusted customer records to S3 as JSON
output_path = "s3://stedi-project/customer_trusted/"
glueContext.write_dynamic_frame.from_options(
    frame=customer_trusted,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "createTable": "true",
        "updateBehavior": "UPDATE_IN_DATABASE",
        "partitionKeys": []
    },
    format="json",
    transformation_ctx="datasink"
)

job.commit()
