# Databricks notebook source
# Databricks notebook source
from pyspark.sql.functions import col, pandas_udf, struct, broadcast
from pyspark.sql.types import *
from ercot.data_eng.src._base import DataEngineering
from ercot.utils.databricks_delta import DatabricksDelta
import logging
import numpy as np
from ercot.base import LMPConfig
import re
from typing import Iterator, Tuple
import pandas as pd
from pyspark.sql.functions import col, pandas_udf, struct

logger = logging.getLogger(__name__)


def create_load_df():  
    """Create input dataframe to be written to bronze table for the corresponding entity

    Args:
        entity_name (str): [Name of entity to run process for i.e. (load, weather, etc.)]
        entity_class (object): [Data engineering class object for corresponding entity]
    """
    try:
        #Create time horizons for the consuming job step (i.e. inference vs training)
        logger.info("Creating initial dataframe load_df")
        load_join_ldf = databricks_source.spark.sql(
            """
            SELECT *
            FROM load_ldf_jaccard
            WHERE jaccard_similarity > 0.5
            """
         )
#         ldf_df.createOrReplaceTempView("ldf_df")
#         logger.info("Write results for load_df to default.load_join_ldf")
#         load_df = databricks_source.spark.sql("""SELECT * 
#                                                  FROM dev_bronze_lmp_forecast.load 
#                                                  WHERE date_time_utc >= (SELECT MIN(date_time) FROM ldf_df)
#                                                  AND date_time_utc <= (SELECT MAX(date_time) FROM ldf_df)
#                                                  """)
#         logger.info("Reading back default.load_join_ldf to dataframe and calculating levenshtein distance")
#         join_df = load_df.join(broadcast(ldf_df), load_df.date_time_utc == ldf_df.date_time)
#         databricks_source.initial_overwrite_load(join_df,"overwrite","date_time_utc","join_df")       
#         load_substation_df = databricks_source.spark.sql(
#           """
#             SELECT b.OBJECTID,
#             b.SUBSTATION as SUBSTATION_WZ,
#             b.NODENAME,
#             b.ZONE,
#             b.ZONEID,
#             b.NODETYPE,
#             b.FIRST_DART_DATE,
#             b.LAST_DART_DATE,
#             b.NEAREST_WEATHERSTATION,
#             b.WEATHERSTATIONID
#             FROM price_node_weather_station_mapping b
#             WHERE b.SUBSTATION IS NOT NULL
#           """
#         )        
        lev_distance_udf = pandas_udf(levenshtein_ratio_and_distance, returnType=FloatType())   
        load_join_ldf = load_join_ldf.withColumn("lev_distance", lev_distance_udf(load_join_ldf.SubStation,load_join_ldf.NODENAME))
        load_join_ldf = load_join_ldf.filter(load_join_ldf.lev_distance > 0.85)
        databricks_source.initial_overwrite_load(load_join_ldf,"overwrite","date_time_utc","load_join_lev_dist")
        logger.info("Unioning dataframe with dataframe containing substation values for both")
        load_join_lev_dist = databricks_source.spark.sql("""SELECT * FROM load_join_lev_dist""")
        load_substation_df = databricks_source.spark.sql(
          """
            SELECT c.date_time_utc,
            a.SubStation,
            a.DistributionFactor,
            a.LoadID,
            a.MVARDistributionFactor, 
            a.MRIDLoad,
            a.DSTFlag,
            b.OBJECTID,
            b.SUBSTATION as SUBSTATION_WZ,
            b.NODENAME,
            b.ZONE,
            b.ZONEID,
            b.NODETYPE,
            b.FIRST_DART_DATE,
            b.LAST_DART_DATE,
            b.NEAREST_WEATHERSTATION,
            b.WEATHERSTATIONID,
            c.ERCOT_LOAD,
            c.HOUSTON_LOAD,
            c.NORTH_ERCOT_LOAD,
            c.SOUTH_LOAD,
            c.WEST_ERCOT_LOAD
            FROM load_distribution_factors a
            JOIN price_node_weather_station_mapping b ON a.SubStation = b.SUBSTATION
            JOIN dev_bronze_lmp_forecast.load c ON to_timestamp(CONCAT(a.LdfDate," ",a.LdfHour),"MM/dd/yyyy HH:mm") = c.date_time_utc
            ORDER BY c.date_time_utc
          """
        )
        load_join_lev_dist = load_join_lev_dist.drop("lev_distance")
        final_df = load_substation_df.union(load_join_lev_dist)
        logger.info("Writing unioned df to load_join_ldf_final")
        databricks_source.initial_overwrite_load(final_df,"overwrite","date_time_utc","load_join_ldf_final")
        return join_df


    
    except Exception as e:
        raise(e)

@pandas_udf("float")
def levenshtein_ratio_and_distance(iterator: Iterator[Tuple[pd.Series, pd.Series]])  -> Iterator[pd.Series]: 
    """ levenshtein_ratio_and_distance:
        Calculates levenshtein distance between two strings.
        If ratio_calc = True, the function computes the
        levenshtein distance ratio of similarity between two strings
        For all i and j, distance[i,j] will contain the Levenshtein
        distance between the first i characters of s and the
        first j characters of t
    """
    ratio_calc = True
    for s, t in iterator:
      # Initialize matrix of zeros
      t = re.sub('[^A-Za-z]+', '', t)
      rows = len(s)+1
      cols = len(t)+1
      distance = np.zeros((rows,cols),dtype = int)

      # Populate matrix of zeros with the indeces of each character of both strings
      for i in range(1, rows):
          for k in range(1,cols):
              distance[i][0] = i
              distance[0][k] = k

      # Iterate over the matrix to compute the cost of deletions,insertions and/or substitutions    
      for col in range(1, cols):
          for row in range(1, rows):
              if s[row-1] == t[col-1]:
                  cost = 0 # If the characters are the same in the two strings in a given position [i,j] then the cost is 0
              else:
                  cost = 2  
              distance[row][col] = min(distance[row-1][col] + 1,      # Cost of deletions
                                  distance[row][col-1] + 1,          # Cost of insertions
                                  distance[row-1][col-1] + cost)     # Cost of substitutions
      # Computation of the Levenshtein Distance Ratio
      ratio = ((len(s)+len(t)) - distance[row][col]) / (len(s)+len(t))
      yield float(ratio)

def main():
    try:
        #Create load_df
        load_df = create_load_df()
        return load_df
                
    except Exception as e:
        raise(e)


if __name__ in "__main__":
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(module)s : %(lineno)d - %(message)s',
                        level=logging.INFO)
    logging.getLogger("py4j").setLevel(logging.ERROR)    
    #Create data source variable to retrieve raw data for features (i.e. DatabricksDelta, AzureBlob, etc.)
    databricks_source = DatabricksDelta(env=LMPConfig.env,dbutils=LMPConfig.dbutils,client=None)
    #Run main method to orchestrate data_engineering
    load_df = main()

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.github.mrpowers.spark.stringmetric.PhoneticAlgorithms._
# MAGIC import org.apache.spark.sql.functions.col
# MAGIC import org.apache.spark.sql.functions.lit
# MAGIC import com.github.mrpowers.spark.stringmetric.SimilarityFunctions._
# MAGIC 
# MAGIC val join_df = spark.sql("""SELECT * FROM join_df""")
# MAGIC val join_iso_df = join_df.withColumn("iso",lit("ERCOT"))
# MAGIC val pricenode_df = spark.sql("""SELECT OBJECTID,
# MAGIC                                 NODENAME,
# MAGIC                                 ZONE,
# MAGIC                                 ZONEID,
# MAGIC                                 NODETYPE,
# MAGIC                                 VOLTAGE,
# MAGIC                                 EQUIPMENT,
# MAGIC                                 FIRST_DART_DATE,
# MAGIC                                 LAST_DART_DATE, ISO as ISO1
# MAGIC                                 FROM price_node_weather_station_mapping 
# MAGIC                                 WHERE SUBSTATION IS NULL""")
# MAGIC val final_df = join_iso_df.join(pricenode_df,
# MAGIC                                    join_iso_df("iso") === pricenode_df("ISO1"),
# MAGIC                                     "inner")
# MAGIC val final_jaccard_df = final_df.withColumn("jaccard_similarity",jaccard_similarity(col("SubStation"),col("NODENAME")))
# MAGIC final_jaccard_df.write.format("delta").saveAsTable("default.load_ldf_jaccard")
# MAGIC 
# MAGIC //val final_cosine_df = final_jaccard_df.withColumn("cosine_similarity",cosine_distance(col("SubStation"),col("NODENAME")))
# MAGIC //#val final_fuzzy_df = final_cosine_df.withColumn("fuzzy_score",fuzzy_score(col("SubStation"),col("NODENAME")))
# MAGIC 
# MAGIC //val join_soundex_df = join_df.withColumn("substation_soundex",refined_soundex(col("SubStation")))
# MAGIC //val pricenode_soundex_df = pricenode_df.withColumn("pricenode_soundex",refined_soundex(col("NODENAME")))
# MAGIC //val final_df = join_soundex_df.join(pricenode_soundex_df,
# MAGIC //                                    join_soundex_df("substation_soundex") === pricenode_soundex_df("pricenode_soundex"),
# MAGIC //                                    "inner")
