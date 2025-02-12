import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import re

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

# Script generated for node Amazon S3
AmazonS3_node1739277776921 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-halah/customer_landing/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1739277776921")

# Script generated for node Change Schema
ChangeSchema_node1739278005656 = ApplyMapping.apply(frame=AmazonS3_node1739277776921, mappings=[("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "bigint", "registrationdate", "long"), ("lastupdatedate", "bigint", "lastupdatedate", "long"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "long"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "bigint"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "long")], transformation_ctx="ChangeSchema_node1739278005656")

# Script generated for node Filter
Filter_node1739277810673 = Filter.apply(frame=ChangeSchema_node1739278005656, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="Filter_node1739277810673")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Filter_node1739277810673, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739277164850", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739278095843 = glueContext.getSink(path="s3://stedi-data-halah/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739278095843")
AmazonS3_node1739278095843.setCatalogInfo(catalogDatabase="stdi",catalogTableName="customer_trusted")
AmazonS3_node1739278095843.setFormat("glueparquet", compression="snappy")
AmazonS3_node1739278095843.writeFrame(Filter_node1739277810673)
job.commit()