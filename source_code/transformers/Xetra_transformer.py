"""Xetra ETL Component"""
import logging
from typing import NamedTuple

from ..common.s3 import S3BucketConnector


class XetraSourceConfig(NamedTuple):
    """
    class for source configuration data
    
    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_isin: column name for is in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volume in source
    """
    
    src_first_extract_date: str
    src_columns: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str
    
    
class XetraTargetConfig(NamedTuple):
    """
    class for target configuration data
    
    trg_col_isin: column name for is in target
    trg_col_date: column name for date in target
    trg_col_op_price: column name for open price in target
    trg_col_close_price: column name for close price in target
    trg_col_min_price: column name for minimmum price in target
    trg_col_max_price: column name for maximum price in target
    trg_col_daily_traded_vol: column name for daily traded volume in taget
    trg_col_ch_prev_close: column name for change to previous day's closing price in taget
    trg_key: basic key of the target file
    trg_key_date_format: date format of taget file key
    trg_format: file formt of the target file
    """
    
    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_close_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_daily_traded_vol: str
    trg_col_ch_prev_close: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str
    
class Xetra_ETL():
    """
    Read the Xetra data, transform, and writes the transformed to the target
    """
    
    def __init__(self, s3_bucket_src: S3BucketConnector, s3_bucket_trg: S3BucketConnector,
                 meta_key: str, src_arg: XetraSourceConfig, trg_arg: XetraTargetConfig):
        """
        Constructor for the XetraTransformer

        Args:
            s3_bucket_src (S3BucketConnector): connection to source s3 bucket
            s3_bucket_trg (S3BucketConnector): connection to target s3 bucket
            meta_key (str): key of meta file
            src_arg (XetraSourceConfig): NamedTuple class with source config data
            trg_arg (XetraTargetConfig): NamedTuple class with target config data
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_arg = src_arg
        self.trg_arg = trg_arg
        self.extract_date = 
        self.extract_date_list = 
        self.meta_update_list =  
        
        def extract():
            pass
        
        def transform():
            pass
        
        def load():
            pass
        