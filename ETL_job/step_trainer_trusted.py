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

# Script generated for node customers_curated
customers_curated_node1739295946113 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-data-halah/customers_curated_2/"], "recurse": True}, transformation_ctx="customers_curated_node1739295946113")

# Script generated for node Amazon S3
AmazonS3_node1739295974818 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-halah/step_trainer_landing/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1739295974818")

# Script generated for node Join
customers_curated_node1739295946113DF = customers_curated_node1739295946113.toDF()
AmazonS3_node1739295974818DF = AmazonS3_node1739295974818.toDF()
Join_node1739295997832 = DynamicFrame.fromDF(customers_curated_node1739295946113DF.join(AmazonS3_node1739295974818DF, (customers_curated_node1739295946113DF['serialnumber'] == AmazonS3_node1739295974818DF['serialnumber']), "left"), glueContext, "Join_node1739295997832")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=Join_node1739295997832, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739294691020", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1739296086087 = glueContext.getSink(path="s3://stedi-data-halah/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1739296086087")
step_trainer_trusted_node1739296086087.setCatalogInfo(catalogDatabase="stdi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1739296086087.setFormat("glueparquet", compression="snappy")
step_trainer_trusted_node1739296086087.writeFrame(Join_node1739295997832)
job.commit()