"""Methods for processing the meta data"""

import pandas as pd
from datetime import datetime, timedelta

from s3 import S3BucketConnector
from constants import MetaProcessFormat
from custom_exceptions import WrongMetaFileException


class MetaProcess():
    """
    This class is for working with the meta data
    """
    @staticmethod
    def update_meta_file(extract_date_list: list, meta_key: str, s3_target_bucket: S3BucketConnector):
        """
        Updating the meta file with the processed Xetra dates and today's date as 
        processed date

        Args:
            extract_date_list (list): a list of dates that are extracted from the source 
            meta_key (str): key of meta file on the s3 bucket
            s3_target_bucket (S3BucketConnector): S3BucketConnector with the bucket containing the meta file
        """
        # Creating an empty dataframe
        df_new = pd.DataFrame(columns=
                              [MetaProcessFormat.META_SOURCE_DATE_COL.value, 
                               MetaProcessFormat.META_PROCESS_DATE_COL.value])
        # Filling the date column with extract_date_list
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        # Filling the processed column
        df_new[MetaProcessFormat.META_PROCESS_DATE_COL.value] = \
            datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            # If the meta file exists
            df_old = s3_target_bucket.read_csv_to_df(meta_key)
            
            if (df_old.columns != df_new.columns).any():
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        except s3_target_bucket.session.client('s3').exceptions.NoSuchKey:
            # No meta file exists
            df_all = df_new
        # Writing to s3
        s3_target_bucket.write_df_s3(df_all, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)
        return True
            
    
    @staticmethod
    def return_date_list(first_date: str, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Creating a list of dates based on the input first_date and the processed dates
        in the meta file

        Args:
            first_date (str): the earliest date Xetra data should be processed
            meta_key (str): ket of the meta file on the s3 bucket
            s3_bucket_meta (S3BucketConnector): S3BucketConnector for the bucket with the meta file

        Returns:
            return_min_date: first date that should be processed
            date_list: list of all dates from min_date till 10 days later
        """
        start = datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date() - timedelta(days=1)
        end = datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date() + timedelta(days=10)
        
        try:
            # If meta file exists create return_date_list using the content of the meta file 
            # Reading meta file
            meta_data = s3_bucket_meta.read_csv_to_df(meta_key)
            # Creating a list of dates from first_date untill the the end_date
            date_list = [datetime.strftime(start+timedelta(days=x), MetaProcessFormat.META_DATE_FORMAT.value) \
            for x in range(0, ((end-start).days))]
            # Creating set of all dates in meta file
            meta_data_date = list(pd.to_datetime(meta_data[MetaProcessFormat.META_SOURCE_DATE_COL.value]).dt.date)
            date_list_date = [datetime.strptime(date, MetaProcessFormat.META_DATE_FORMAT.value).date()\
                for date in date_list]
            not_processed = [date for date in date_list_date if date not in meta_data_date]
            if not_processed:
                min_date = min(not_processed)
                
                date_list = [datetime.strftime(date, MetaProcessFormat.META_DATE_FORMAT.value) \
                    for date in date_list_date if date>=min_date]
                return_min_date = min(date_list)
            else:
                # Setting values for the earliest date and the list of dates
                date_list = []
                return_min_date = datetime(2200, 1, 1).date()\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # No meta file found -> creating a date list from first_date - 1 day untill the end date
            return_min_date = start
            date_list = [
              (start + timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
              for x in range(0, (end - start).days)]
            
            
        return return_min_date, date_list

            
    