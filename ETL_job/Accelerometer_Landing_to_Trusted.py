import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_trusted
customer_trusted_node1739288420476 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-data-halah/customer_trusted/run-1739278786952-part-block-0-r-00000-snappy.parquet"]}, transformation_ctx="customer_trusted_node1739288420476")

# Script generated for node accelerometer landing
accelerometerlanding_node1739288460117 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-halah/accelerometer_landing/landing/"], "recurse": True}, transformation_ctx="accelerometerlanding_node1739288460117")

# Script generated for node Join
accelerometerlanding_node1739288460117DF = accelerometerlanding_node1739288460117.toDF()
customer_trusted_node1739288420476DF = customer_trusted_node1739288420476.toDF()
Join_node1739288520476 = DynamicFrame.fromDF(accelerometerlanding_node1739288460117DF.join(customer_trusted_node1739288420476DF, (accelerometerlanding_node1739288460117DF['user'] == customer_trusted_node1739288420476DF['email']), "right"), glueContext, "Join_node1739288520476")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1739288520476, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739288054644", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739288667218 = glueContext.getSink(path="s3://stedi-data-halah/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739288667218")
AmazonS3_node1739288667218.setCatalogInfo(catalogDatabase="stdi",catalogTableName="accelerometer_trusted")
AmazonS3_node1739288667218.setFormat("glueparquet", compression="snappy")
AmazonS3_node1739288667218.writeFrame(Join_node1739288520476)
job.commit()