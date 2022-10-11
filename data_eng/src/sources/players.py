import datetime
import logging
from re import S
#import pyspark.sql.functions as f
#from pyspark.sql import DataFrame
from data_eng.src._base import DataEngineering
from base import Config
import requests
import pandas as pd
import requests
import json

logger = logging.getLogger(__name__)

class PlayersData(DataEngineering):
    """[Class for data engineering operations required for ercot weather data from wsi]
    """
    def __init__(self, target_database:str, sql_client:object):
        self.target_database = target_database
        self.target_table = f'players'
        self.sql_client = sql_client

    def extract_data(self):
        """Create data from api call to NHL teams endpoint
        """
        sql = "select distinct(playerId) as playerId from dbo.team_rosters"
        player_ids_df = pd.read_sql(sql,con=self.sql_client.engine)
        player_list = []
        for player_id in list(player_ids_df['playerId']):
            player_response = requests.get(f"{Config.API_URL.value}/people/{player_id}").json()
            player_dict = player_response['people'][0]
            #Remove the follow keys
            if 'primaryNumber' in player_dict.keys(): 
                player_dict['primaryNumber']=int(player_dict['primaryNumber'])
            else:
                player_dict['primaryNumber']=None
            player_dict['birthDate'] = pd.to_datetime(player_dict['birthDate'],format='%Y-%m-%d')
            if 'currentTeam' in player_dict.keys():
                player_dict['currentTeamID']=player_dict['currentTeam']['id']
            else:
                player_dict['currentTeamID']=None
            if 'primaryPosition' in player_dict.keys():
                player_dict['position']=player_dict['primaryPosition']['abbreviation']
            else:
                player_dict['position']=None
            remove_keys = ['link','currentTeam','primaryPosition']
            for key in remove_keys:
                player_dict.pop(key, None)
            player_list.append(player_dict)
        return player_list 


    def transform_data(self, input_list: object):
        """Method to perform the required pivots or aggregations on the dataframe before loading

        Args:
            input_lis (list): [Input list containing response from API]
        """
        #Transform the required elements and create transformed dataframe
        df = pd.DataFrame.from_records(input_list)
        return df


    def load_data(self, transformed_df: object):
        """Method to load data to its corresponding table in the bronze table
        """
        self.sql_client.write_data(transformed_df, self.target_table)