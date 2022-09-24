import datetime
import logging
from re import S
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from data_eng.src._base import DataEngineering
from base import Config
import requests
import pandas as pd
import requests

logger = logging.getLogger(__name__)

class TeamsData(DataEngineering):
    """[Class for data engineering operations required for ercot weather data from wsi]
    """
    def __init__(self, databricks_delta:object, job_step:str, target_database:str):
        self.databricks_delta = databricks_delta
        self.job_step = job_step
        self.target_database = target_database
        self.target_table = f'{target_database}.teams'

    def extract_data(self):
        """Create data from api call to NHL teams endpoint
        """
        teams = requests.get(f"{Config.API_URL.value}/teams")
        

        


    def transform_data(self, input_df: DataFrame):
        """Method to perform the required pivots or aggregations on the dataframe before loading

        Args:
            input_df (DataFrame): [Input dataframe containing extract to be transformed]
        """
        #Transform the required elements and create transformed dataframe
        pass


    def load_data(self, transformed_df: DataFrame):
        """Method to load data to its corresponding table in the bronze table
        """
        write_mode='overwrite'
        select_expr = [f.col(column).cast("float") if data_type == "string" and column != 'source_type' else f.col(column) for column, data_type in transformed_df.dtypes]
        transformed_df = transformed_df.select(*select_expr)        
        self.databricks_delta.initial_overwrite_load(transformed_df,write_mode,"create_timestamp_utc",f"{self.target_table}")          
        self.databricks_delta.spark.sql(f"""OPTIMIZE {self.target_table}""")