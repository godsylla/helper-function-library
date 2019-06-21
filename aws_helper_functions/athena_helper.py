import boto3
import botocore
import io
import time
import pandas as pd 

s3_client = boto3.client('s3', region_name='us-west-2')
s3_resource = boto3.resource('s3', region_name='us-west-2')

athena_client = boto3.client('athena', region_name='us-west-2')


# Function to query Athena
def run_query (query:str, database:str, s3_output_location:str):
    assert 's3://' in s3_output_location, "s3_output_location must have prefix s3://"

    response = athena_client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = {
            'Database': database
        },
        ResultConfiguration = {
            'OutputLocation': s3_output_location
        }
    )

    execution_id = response['QueryExecutionId']
    return execution_id, response


# Execute query
def execute_query (query:str, database:str, s3_output_location:str):

    execution_id, _ = run_query(query=query, database=database, s3_output_location=s3_output_location)

    while True:
        stats = athena_client.get_query_execution(QueryExecutionId=execution_id)
        status = stats['QueryExecution']['Status']['State']
        print(status)

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(10)


# Retrieve results written to S3 output location and return a pandas DataFrame
def return_query_results_as_df (bucket_name:str, query_execution_id:str):
    obj = s3_client.get_object(Bucket=bucket_name, Key=f'{query_execution_id}.csv')

    return pd.read_csv(io.BytesIO(obj['Body'].read()))

        
# Function to clear the S3 bucket after the queries are run
def clean_bucket (bucket_name: str):
    assert 's3://' not in bucket_name, 'The s3:// prefix is not needed in the bucket name.'
    my_bucket = s3_resource.Bucket(bucket_name)
    my_bucket.objects.all().delete()


# TODO: Write test code for the various functions (instead of stashing it all into the main() function)
