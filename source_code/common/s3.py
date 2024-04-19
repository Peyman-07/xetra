"""Connector and Methods Accessing S3"""

import os
import logging
import sys
sys.path.append("/Users/pashkrof/surfdrive_general/DITTLAB/Courses/Data pipeline/production-ready ETL/venv/xetra/source_code/common")
from io import StringIO, BytesIO
import boto3
import pandas as pd
from custom_exceptions import WrongFormatException
from constants import S3FileTypes




class S3BucketConnector():
    """
    Class for interacting with s3 buckets
    """

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector

        Args:
            access_key (str): aceess key for accessing s3
            secret_key (str): secret key for accessing s3
            endpoint_url (str): endpoint url for s3
            bucket (str): s3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])
        self.secret_key = secret_key
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)
        self.s3_client = boto3.client('s3', region_name='eu-north-1', endpoint_url=endpoint_url)
    


    def list_files_in_prefix(self, prefix: str):
        """
        listing all files with a prefix on the S3 bucket

        :param prefix: prefix on the S3 bucket that should be filtered with

        returns:
          files: list of all the file names containing the prefix in the key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files
    

    def read_csv_to_df(self, key: str, decoding: str = 'utf-8', sep: str = ','):
        """
        Reading a CSv file from the S3 bucket and returning a dataframe

        Args:
            key (str): key of the file that shoudl be read
            decoding (str, optional): encoding of the data inside the csv file
            sep (str, optional): separator of the csv file
            
        Returns:
            df: Pandas DateaFrame containing the data of the csv file
        """
        self._logger.info('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, sep=sep)
        return data_frame


    def write_df_s3(self, df: pd.DataFrame, key: str, file_format: str):
        """
        Write a dataframe to the S3 bucket
        Supported formats: .parquet, .csv

        Args:
            df (pd.DataFrame): dataframe
            key (str): target key
            file_format (str): format of the file
        """
        if df.empty:
            self._logger.info('The dataframe is empty! No file will be written.')
            return None
            
        self._logger.info(f'Writing file {self.endpoint_url}, {self._bucket.name}, {key}')
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
            return True
        elif file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)
            self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
            return True
        else: 
            self._logger.info(f'The file type {file_format} is not supported!')
            raise WrongFormatException
        
