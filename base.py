#from pyspark.sql import SparkSession
from enum import Enum
import os


class Config(Enum):
    """[Base class for locational marginalized pricing project Contains structure to
        onduct time-series forecasting across several different ISOs, market conditions and asset types

    Args:
        dbutils (object): [DButils object to perform file-system operations in databricks]
        env (str): [Environment variable (i.e. dev, qa, prod)]
        bronze_database (str): [Database containing raw ingested data for each asset type and target price data]
        silver_database (str): [Database containing engineered features for each asset type]
        gold_database (str): [Database containing model inference results for each price variable]
    """    
    #class variables
    #spark = SparkSession.builder.getOrCreate()
    #ENV = os.environ['env']
    DATABASE = f"hockey_analytics"
    HISTORICAL_CUTOFF_DATE = "2001-08-01"
    API_URL = "https://statsapi.web.nhl.com/api/v1"