# Databricks notebook source
import logging
from base import Config
from utils.sql import SQL
from data_eng.src.sources.team_rosters import TeamRosters
from data_eng.src.sources.teams import TeamsData
from data_eng.src.sources.seasons import SeasonsData
from data_eng.src.sources.players import PlayersData
from data_eng.src.sources.games import GamesData
from data_eng.src.sources.game_events import GameEventsData
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
        datsource_class = entity_class(Config.DATABASE.value, sql_client)           
        logger.info(f"Running extract process for {entity_name}")
        extract_df = datsource_class.extract_data()
        logger.info(f"Running transform process for {entity_name}")            
        transformed_df = datsource_class.transform_data(extract_df)
        logger.info(f"Loading transformed {entity_name} data to bronze table")
        datsource_class.load_data(transformed_df)            
    
    except Exception as e:
        logger.error(f"Error running data_engineering process for {entity_name}")
        raise(e)


def main():
    try:
        #Source table dictionary containing child classes for each entity
        source_tables_dict = {  
                                #'team_rosters':TeamRosters, 
                                'game_events':GameEventsData,
                                'games':GamesData,
                                'players':PlayersData,
                                'teams': TeamsData,
                                'seasons':SeasonsData
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
    #Create data source variable to retrieve raw data for features (i.e. DatabricksDelta, AzureBlob, etc.)
    sql_client = SQL(database=Config.DATABASE.value)
    #Run main method to orchestrate data_engineering
    main()