from pyspark.sql import SparkSession
from enum import Enum
import os

def get_configs(spark):
    """Get configuration for dbutils and environment

    Args:
        spark (SparkSession): [Spark session variable]
    """    
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        env = os.environ['ENV']

    except KeyError:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        env = dbutils.widgets.get("ENV")
    return dbutils, env    

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
    spark = SparkSession.builder.getOrCreate()
    dbutils, ENV = get_configs(spark)
    BRONZE_DATABASE = f"{ENV}_bronze_hockey_analytics"
    SILVER_DATABASE = f"{ENV}_silver_hockey_analytics"
    GOLD_DATABASE = f"{ENV}_gold_hockey_analytics"
    TIME_COLUMN_NAME = "date_time_utc"
    HISTORICAL_CUTOFF_DATE = "2018-01-01 00:00:00"
    API_URL = "https://statsapi.web.nhl.com/api/v1"