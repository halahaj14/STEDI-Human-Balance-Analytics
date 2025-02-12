import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

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
customer_trusted_node1739291591082 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-data-halah/customer_trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1739291591082")

# Script generated for node accelerometer_landing
accelerometer_landing_node1739291612376 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-data-halah/accelerometer_trusted/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1739291612376")

# Script generated for node Join
customer_trusted_node1739291591082DF = customer_trusted_node1739291591082.toDF()
accelerometer_landing_node1739291612376DF = accelerometer_landing_node1739291612376.toDF()
Join_node1739291939107 = DynamicFrame.fromDF(customer_trusted_node1739291591082DF.join(accelerometer_landing_node1739291612376DF, (customer_trusted_node1739291591082DF['email'] == accelerometer_landing_node1739291612376DF['email']), "outer"), glueContext, "Join_node1739291939107")

# Script generated for node Change Schema
ChangeSchema_node1739292132319 = ApplyMapping.apply(frame=Join_node1739291939107, mappings=[("registrationdate", "bigint", "registrationdate", "long"), ("customername", "string", "customername", "string"), ("birthday", "string", "birthday", "string"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "long"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "long"), ("lastupdatedate", "bigint", "lastupdatedate", "long"), ("email", "string", "email", "string"), ("serialnumber", "string", "serialnumber", "string"), ("phone", "string", "phone", "string"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "bigint"), ("timestamp", "bigint", "timestamp", "long"), ("registrationdate", "bigint", "registrationdate", "long"), ("customername", "string", "customername", "string"), ("birthday", "string", "birthday", "string"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "long"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "long"), ("lastupdatedate", "bigint", "lastupdatedate", "long"), ("email", "string", "email", "string"), ("serialnumber", "string", "serialnumber", "string"), ("phone", "string", "phone", "string"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "bigint")], transformation_ctx="ChangeSchema_node1739292132319")

# Script generated for node Filter
Filter_node1739292662229 = Filter.apply(frame=ChangeSchema_node1739292132319, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="Filter_node1739292662229")

# Script generated for node Drop Duplicates
DropDuplicates_node1739293698717 =  DynamicFrame.fromDF(Filter_node1739292662229.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1739293698717")

# Script generated for node customers_curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1739293698717, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739291507727", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customers_curated_node1739292298908 = glueContext.getSink(path="s3://stedi-data-halah/customers_curated_2/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customers_curated_node1739292298908")
customers_curated_node1739292298908.setCatalogInfo(catalogDatabase="stdi",catalogTableName="customers_curated_2")
customers_curated_node1739292298908.setFormat("glueparquet", compression="snappy")
customers_curated_node1739292298908.writeFrame(DropDuplicates_node1739293698717)
job.commit()