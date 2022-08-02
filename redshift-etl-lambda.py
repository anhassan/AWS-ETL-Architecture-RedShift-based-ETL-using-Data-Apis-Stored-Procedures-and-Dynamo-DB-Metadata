import json
import boto3
import os
from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    
   print("Lambda Function Got Triggered")
    
   # Defining parameters for the lambda job
   source = ""
   region = "us-west-2"
   meta_table_name = "redshiftetlconfig"
    
   # Reading configurations from the environment variables
   AWS_ACCESS_KEY_ID = os.environ["aws_access_key_id"]
   AWS_SECRET_ACCESS_KEY = os.environ["aws_secret_access_key"]
   REDSHIFT_CLUSTER = os.environ["redshift_cluster"]
   REDSHIFT_DB = os.environ["redshift_database"]
   REDSHIFT_SECRET = os.environ["redshift_secret"]
   
   if event["detail-type"] == "Redshift Data Statement Status Change":
       source = event["detail"]["statementName"]
       print("Source Statement : {} ".format(source))
   if event["detail-type"] == "Glue Job State Change":
       source = event["detail"]["jobName"]
       print("Source Job : {}".format(source))
   state = event["detail"]["state"]
   print("Status of Event : {}".format(state))
   
   # Reading the ETL metadata information from dynamo db table
   dd_client = boto3.resource("dynamodb",region_name=region,aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
   dd_table = dd_client.Table(meta_table_name)
   response = dd_table.query(KeyConditionExpression=Key('Source').eq(source))
   target = response["Items"][0]["Target"]
   target_type = response["Items"][0]["TargetType"]
   target_sql = response["Items"][0]["TargetSql"]
   
   # Log details for reference
   print("Target Type : {}".format(target_type))
   print("Target SQL : {}".format(target_sql))
   
   # Orchestrating stored procedures in redshift to perform ETL operations
   redshift_data_client = boto3.client("redshift-data",region_name=region,aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
   
   if target_type == "sql" and (state == "SUCCEEDED" or state == "FINISHED" or state == "COMPLETE"):
       redshift_data_client.execute_statement(ClusterIdentifier=REDSHIFT_CLUSTER,Database=REDSHIFT_DB,
                                     SecretArn=REDSHIFT_SECRET,Sql=target_sql,
                                     StatementName=target,WithEvent=True)
                                     
