# Functions that interact with different AWS Resources

import pandas as pd 
import numpy as np 

import boto3 
import s3fs
import pickle

import pyarrow as pa
import pyarrow.parquet as pq



def save_to_s3 (object_to_save, filename:str, s3_bucketname:str, s3_bucketkey=None):
    '''
    INPUT: object_to_save
    INPUT: filename - needs to have the ".fileformat" as the suffix of the filename
    INPUT: s3_bucketname - does not need the prefix "s3://" or the suffix "/" 
            e.g. gn5455-coin-datascience
    INPUT: s3_bucketkey - the file folder within the bucket you want to write the object to
    '''
    if s3_bucketkey is not None:
        key_path = s3_bucketkey + '/' + filename
    else:
        key_path = '/' + filename
        
    try:
        s3_resource = boto3.resource('s3')
        my_object = s3_resource.Object(s3_bucketname, Key=key_path)
        my_object.put(Body=pickle.dumps(object_to_save))
    except:
        s3_client = boto3.client('s3')
        s3_client.put_object(Body=pickle.dumps(object_to_save), Bucket=s3_bucketname, Key=key_path)


def save_df_to_s3 (df:pd.DataFrame, filename:str, s3_bucketname:str, s3_bucketkey=None):
    '''
    INPUT: object_to_save
    INPUT: filename - needs to have the ".fileformat" as the suffix of the filename
    INPUT: s3_bucketname - does not need the prefix "s3://" or the suffix "/" 
    INPUT: s3_bucketkey - the file folder within the bucket you want to write the object to
    '''
    path_to_use = 's3://' + s3_bucketname + '/' + s3_bucketkey + '/' + filename
    bytes_to_write = df.to_csv(None).encode()
    fs = s3fs.S3FileSystem()
    with fs.open(path_to_use, 'wb') as f:
        f.write(bytes_to_write)


def write_df_to_parquet_to_s3 (df:pd.DataFrame, filename:str, s3_bucketname:str, s3_bucketkey=None):
    assert 's3://' not in s3_bucketname, 'prefix "s3://" not required'
    assert filename[-8:] == '.parquet', 'filename must have suffix ".parquet"'
    assert '/' not in s3_bucketkey, '"/" not required for s3_bucketkey argument'

    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename)

    if s3_bucketkey is not None:
        key_to_use = s3_bucketkey + '/' + filename
    else:
        key_to_use = filename

    s3_client = boto3.client('s3')
    
    with open(filename) as f:
        object_data = f.read()
        s3_client.put_object(Body=object_data, Bucket=s3_bucketname, Key=key_to_use)


