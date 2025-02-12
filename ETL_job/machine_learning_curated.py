import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1739296510112 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-data-halah/accelerometer_trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1739296510112")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1739296533865 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-data-halah/step_trainer_trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1739296533865")

# Script generated for node Join
accelerometer_trusted_node1739296510112DF = accelerometer_trusted_node1739296510112.toDF()
step_trainer_trusted_node1739296533865DF = step_trainer_trusted_node1739296533865.toDF()
Join_node1739296590741 = DynamicFrame.fromDF(accelerometer_trusted_node1739296510112DF.join(step_trainer_trusted_node1739296533865DF, (accelerometer_trusted_node1739296510112DF['timestamp'] == step_trainer_trusted_node1739296533865DF['sensorreadingtime']), "left"), glueContext, "Join_node1739296590741")

# Script generated for node Change Schema
ChangeSchema_node1739297115580 = ApplyMapping.apply(frame=Join_node1739296590741, mappings=[("user", "string", "user", "string"), ("timestamp", "bigint", "timestamp", "long"), ("x", "double", "x", "double"), ("y", "double", "y", "double"), ("z", "double", "z", "double"), ("registrationdate", "bigint", "registrationdate", "long"), ("customername", "string", "customername", "string"), ("birthday", "string", "birthday", "string"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "long"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "long"), ("lastupdatedate", "bigint", "lastupdatedate", "long"), ("email", "string", "email", "string"), ("serialnumber", "string", "serialnumber", "string"), ("phone", "string", "phone", "string"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "bigint"), ("registrationdate", "bigint", "registrationdate", "long"), ("customername", "string", "customername", "string"), ("birthday", "string", "birthday", "string"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "long"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "long"), ("lastupdatedate", "bigint", "lastupdatedate", "long"), ("email", "string", "email", "string"), ("serialnumber", "string", "serialnumber", "string"), ("phone", "string", "phone", "string"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "bigint"), ("sensorreadingtime", "bigint", "sensorreadingtime", "bigint"), ("serialnumber#0", "string", "serialnumber#0", "string"), ("distancefromobject", "int", "distancefromobject", "int")], transformation_ctx="ChangeSchema_node1739297115580")

# Script generated for node Filter
Filter_node1739297137755 = Filter.apply(frame=ChangeSchema_node1739297115580, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="Filter_node1739297137755")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=Filter_node1739297137755, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739294691020", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1739297366888 = glueContext.getSink(path="s3://stedi-data-halah/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1739297366888")
machine_learning_curated_node1739297366888.setCatalogInfo(catalogDatabase="stdi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1739297366888.setFormat("glueparquet", compression="snappy")
machine_learning_curated_node1739297366888.writeFrame(Filter_node1739297137755)
job.commit()