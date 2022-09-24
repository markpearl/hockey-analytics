# Databricks notebook source
import logging
from base import Config
from utils.databricks_delta import DatabricksDelta
from data_eng.src.sources.teams import TeamsData
import os

logger = logging.getLogger(__name__)


def run_data_engineering(entity_name:str, entity_class:object):  
    """Create input dataframe to be written to bronze table for the corresponding entity

    Args:
        entity_name (str): [Name of entity to run process for i.e. (load, weather, etc.)]
        entity_class (object): [Data engineering class object for corresponding entity]
    """
    try:
        #Call corresponding method for the entity
        datsource_class = entity_class(databricks_source, job_step, Config.BRONZE_DATABASE.value)           
        logger.info(f"Running extract process for {entity_name} data for: {job_step} step")
        extract_df = datsource_class.extract_data()
        logger.info(f"Running transform process for {entity_name} data for: {job_step} step")            
        transformed_df = datsource_class.transform_data(extract_df)
        logger.info(f"Loading transformed {entity_name} data to bronze table for: {job_step} step")
        datsource_class.load_data(transformed_df)            
    
    except Exception as e:
        logger.error(f"Error running data_engineering process for {entity_name}")
        raise(e)


def main():
    try:
        #Source table dictionary containing child classes for each entity
        source_tables_dict = {  
                                'teams': TeamsData
                            }
        #Start engineering process
        for entity_name, entity_class in source_tables_dict.items():
            run_data_engineering(entity_name, entity_class)
                
    except Exception as e:
        raise(e)

if __name__ in "__main__":
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(module)s : %(lineno)d - %(message)s',
                        level=logging.INFO)
    logging.getLogger("py4j").setLevel(logging.ERROR)    
    #Read job parameter for job_step
    try:
        job_step = os.environ['job_step']

    except KeyError:
        job_step = Config.dbutils.value.widgets.get("job_step")
    #Create data source variable to retrieve raw data for features (i.e. DatabricksDelta, AzureBlob, etc.)
    databricks_source = DatabricksDelta(env=Config.ENV.value,dbutils=Config.dbutils.value,client=None)
    #Run main method to orchestrate data_engineering
    main()