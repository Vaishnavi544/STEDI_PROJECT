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

# Script generated for node customer_landing
customer_landing_node1768971819631 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-uda-project/landing/customer_landing/"], "recurse": True}, transformation_ctx="customer_landing_node1768971819631")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource
where shareWithResearchAsOfDate IS NOT NULL;
'''
SQLQuery_node1768971823092 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customer_landing_node1768971819631}, transformation_ctx = "SQLQuery_node1768971823092")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1768971823092, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1768971763016", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1768971825664 = glueContext.getSink(path="s3://stedi-uda-project/landing/customer_landing/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1768971825664")
customer_trusted_node1768971825664.setCatalogInfo(catalogDatabase="my_db",catalogTableName="customer_trusted")
customer_trusted_node1768971825664.setFormat("json")
customer_trusted_node1768971825664.writeFrame(SQLQuery_node1768971823092)
job.commit()
