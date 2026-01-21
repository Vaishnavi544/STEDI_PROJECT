import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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
accelerometer_trusted_node1768973088768 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-uda-project/landing/accelerometer_landing/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1768973088768")

# Script generated for node customer_trusted
customer_trusted_node1768973090716 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-uda-project/landing/customer_landing/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1768973090716")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from a join c on a.user=c.email;

'''
SQLQuery_node1768973093934 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"c":customer_trusted_node1768973090716, "a":accelerometer_trusted_node1768973088768}, transformation_ctx = "SQLQuery_node1768973093934")

# Script generated for node SQL Query
SqlQuery1 = '''
select distinct
 serialnumber,
sharewithpublicasofdate,
birthday,
registrationdate,
sharewithresearchasofdate,
customername,
email,
lastupdatedate,
phone,
sharewithfriendsasofdate
from myDataSource;

'''
SQLQuery_node1768973095370 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":SQLQuery_node1768973093934}, transformation_ctx = "SQLQuery_node1768973095370")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1768973095370, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1768973060146", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1768973099281 = glueContext.getSink(path="s3://stedi-uda-project/landing/customer_landing/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1768973099281")
customer_curated_node1768973099281.setCatalogInfo(catalogDatabase="my_db",catalogTableName="customer_curated")
customer_curated_node1768973099281.setFormat("json")
customer_curated_node1768973099281.writeFrame(SQLQuery_node1768973095370)
job.commit()
