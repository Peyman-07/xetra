"""TestXetraTransfomerMethods"""

import os
import unittest
from io import StringIO
from datetime import datetime, timedelta

import boto3
import pandas as pd
from moto import mock_aws

import sys
sys.path.append("/Users/pashkrof/surfdrive_general/DITTLAB/Courses/Data pipeline/production-ready ETL/venv/xetra/source_code/common")


from s3 import S3BucketConnector
from meta_process import MetaProcess
from constants import MetaProcessFormat
from custom_exceptions import WrongMetaFileException


class TestXetraETLMethods(unittest.TestCase):
    """
    Testing the XetraETL class.
    """

    def setUp(self):
        """
        Setting up the environment
        """
        # mocking s3 connection start
        self.mock_s3 = mock_aws()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Creating a bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                                  CreateBucketConfiguration={
                                      'LocationConstraint': 'eu-central-1'})
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating a S3BucketConnector instance
        self.s3_bucket_meta = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)
        self.dates = [(datetime.today().date() - timedelta(days=day))\
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(8)]

    def tearDown(self):
        # mocking s3 connection stop
        self.mock_s3.stop()