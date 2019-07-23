import boto3
import botocore
import io
import time
import pandas as pd 
import random

s3_client = boto3.client('s3', region_name='us-west-2')
s3_resource = boto3.resource('s3', region_name='us-west-2')

athena_client = boto3.client('athena', region_name='us-west-2')


def execute_athena_query (query_string:str, database_name:str, s3_output_location:str, random_sleep_bounds=(4,8)) -> dict:
    assert 's3://' in s3_output_location, "s3_output_locaton requires the prefix 's3://'."
    
    athena_client = boto3.client('athena')
    
    response = athena_client.start_query_execution(QueryString=query_string,
                                                   QueryExecutionContext={
                                                       'Database' : database_name
                                                   },
                                                   ResultConfiguration={
                                                       'OutputLocation': s3_output_location
                                                   })
    execution_id = response['QueryExecutionId']
    
    while True:
        res = athena_client.get_query_execution(QueryExecutionId=execution_id)
        status = res['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status == 'SUCCEEDED':
                return res
            else:
                raise ValueError(res)
        time.sleep(random.uniform(*random_sleep_bounds))

    # return response


def query_results_to_csv_to_df (athena_query_response:dict, bucket_name:str, **s3_key:str) -> pd.DataFrame:

    # make function ambiv to if the s3:// prefix is in the argument
    if bucket_name[:5] == 's3://':
        bucket_name = bucket_name[5:]
    else:
        bucket_name
        
    s3_client = boto3.client('s3')
    
    query_exec_id = athena_query_response['QueryExecution']['QueryExecutionId'] 
    filename = query_exec_id + '.csv'
    
    key = s3_key.get('s3_key', '')
    if key[-1] == '/':
        key_path = key + filename
    else:
        key_path = key + '/' + filename
    
    obj = s3_client.get_object(Bucket=bucket_name,
                              Key=key_path)
    
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df
     
# # TODO: Write test code for the various functions (instead of stashing it all into the main() function)
