import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Creating Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


import boto3

# A helper function to ingest data into s3 through glue job
def ingest_data(bucket_name,src_layer,dst_layer,region):
    s3_resource = boto3.resource("s3",region_name=region)
    s3_bucket = s3_resource.Bucket(bucket_name)
    s3_bucket_objs = s3_bucket.objects.all()
    to_ingest_paths = [obj.key for obj in s3_bucket_objs if src_layer in obj.key]
    
    for path in to_ingest_paths:
            dst_file = path[path.rfind("/")+1:]
            
            if len(dst_file) > 0:
                dst_folder = dst_file[:dst_file.find(".")]
                s3_resource.meta.client.copy({ 'Bucket':bucket_name,
                'Key':path},Bucket=bucket_name,Key="{}/{}/{}".format(dst_layer,dst_folder,dst_file))
                
                print("Ingesting File : {} to Folder : {} in Layer : {}".format(dst_file,dst_folder,dst_layer))


# Parameters required for ingestion
region = "us-west-2"
bucket_name = "datalake-idp"
src_layer = "app-data"
dst_layer = "raw"


# Calling the ingestion function for in
ingest_data(bucket_name,src_layer,dst_layer,region)
