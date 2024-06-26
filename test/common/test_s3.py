"""TestS3BucketConnectorMethods"""

import os
import unittest
from io import StringIO, BytesIO


import sys
sys.path.append("/Users/pashkrof/surfdrive_general/DITTLAB/Courses/Data pipeline/production-ready ETL/venv/xetra/source_code/common")

import boto3
import pandas as pd
from moto import mock_aws

from s3 import S3BucketConnector
from custom_exceptions import WrongFormatException


class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector class
    """
    
    def setUp(self):
        """
        Setting up the environment
        """
        # Mocking s3 connection start
        self.mock_s3 = mock_aws()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test_bucket'
        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_access_key] = 'KEY2'
        # Creating a bicket on the mocked S3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name, CreateBucketConfiguration={
            'LocationConstraint': 'eu-central-1'
        })
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating a testing instance
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key, self.s3_secret_key, self.s3_endpoint_url,self.s3_bucket_name)
    
    def tearDown(self):
        """
        Executing after unittests
        """
        # mocking s3 connection stop
        self.mock_s3.stop()
    
    def test_list_files_in_prefix_ok(self):
        """
        Tests the list_files_in_prefix method for getting 2 file keys
        as list on the mocked s3 bucket
        """
        # Expected results
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'
        # Test init
        csv_content = """col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method execution
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)
        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key2_exp
                    }
                ]
            }
        )
    
    
    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Tests the list_files_in_prefix method in case of a
        wrong or not existing prefix
        """
        # Expected results
        prefix_exp = 'no-prefix/'
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method execution
        self.assertTrue(not list_result)


    def test_read_csv_to_df_ok(self):
        """
        Tests the read_csv_to_df method for
        reading 1 .csv file from the mocked s3 bucket
        """
        # Expected results
        key_exp = 'test.csv'
        col1_exp = 'col1'
        col2_exp = 'col2'
        val1_exp = 'val1'
        val2_exp = 'val2'
        log_exp = f'Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        # Test init
        csv_content = f'{col1_exp},{col2_exp}\n{val1_exp},{val2_exp}'
        self.s3_bucket.put_object(Body=csv_content, Key=key_exp)
        # Method execution
        with self.assertLogs() as logm:
            df_result = self.s3_bucket_conn.read_csv_to_df(key_exp)
            # Log test after method execution
            self.assertIn(log_exp, logm.output[0])
        # Test after method execution
        self.assertEqual(df_result.shape[0], 1)
        self.assertEqual(df_result.shape[1], 2)
        self.assertEqual(val1_exp, df_result[col1_exp][0])
        self.assertEqual(val2_exp, df_result[col2_exp][0])
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )
    
    def test_write_df_to_s3_empty(self):
        """
        Test the write_df_to_s3 method for an empty dataframe 
        """
        # Expected results
        return_exp = None
        log_exp = 'The dataframe is empty! No file will be written.'
        # Test init
        df_empty = pd.DataFrame()
        file_format='csv'
        key='key.csv'
        # Method execution
        with self.assertLogs() as logm:
            df_result = self.s3_bucket_conn.write_df_s3(df_empty, key, file_format)
            # Log test after method execution
            self.assertIn(log_exp, logm.output[0])
        # Test after method execution
        self.assertEqual(df_result,return_exp)
        
        
    def test_write_df_to_s3_csv(self):
        """
        Test the write_df_to_s3 method with csv format
        """
        # Expected results
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns = ['col1', 'col2'])
        key_exp = 'test.csv'
        log_exp = f'Writing file {self.s3_endpoint_url}, {self.s3_bucket.name}, {key_exp}'
        # Test init
        file_format = 'csv'
        # Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_s3(df_exp, key_exp, file_format)
            # Log test after method execution
            self.assertIn(log_exp, logm.output[0])
        # Test after method execution
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )
        
    def test_write_df_to_s3_parquet(self):
        """
        Test the write_df_to_s3 method with parquet format
        """
        # Expected results
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns = ['col1', 'col2'])
        key_exp = 'test.parquet'
        log_exp = f'Writing file {self.s3_endpoint_url}, {self.s3_bucket.name}, {key_exp}'
        # Test init
        file_format = 'parquet'
        # Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_s3(df_exp, key_exp, file_format)
            # Log test after method execution
            self.assertIn(log_exp, logm.output[0])
        # Test after method execution
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )


    def test_write_df_to_s3_wrong_format(self):
        """
        Test the write_df_to_s3 method with a wrong format
        """
         # Expected results
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns = ['col1', 'col2'])
        key_exp = 'test.parquet'
        format_exp = 'wrong_format'
        log_exp = f'The file type {format_exp} is not supported!'
        exception_exp = WrongFormatException
        # Method execution
        with self.assertLogs() as logm:
            with self.assertRaises(exception_exp):
                self.s3_bucket_conn.write_df_s3(df_exp, key_exp, format_exp)
                # Log test after method execution
                self.assertIn(log_exp, logm.output[0])
    
if __name__ == "__main__":
    unittest.main()