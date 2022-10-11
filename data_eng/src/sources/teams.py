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

class TeamsData(DataEngineering):
    """[Class for data engineering operations required for ercot weather data from wsi]
    """
    def __init__(self, target_database:str, sql_client:object):
        self.target_database = target_database
        self.target_table = f'teams'
        self.sql_client = sql_client

    def extract_data(self):
        """Create data from api call to NHL teams endpoint
        """
        response = requests.get(f"{Config.API_URL.value}/teams").json()
        teams_list = []
        for team in response['teams']:
            #Extract the desired element from the teams API
            team['divisionID']=team['division']['id']
            team['divisionName']=team['division']['name']
            team['conferenceID']=team['conference']['id']
            team['conferenceName']=team['conference']['name']
            #Remove the follow keys
            remove_keys = ['venue','division','conference','franchise','link','officialSiteUrl']
            for key in remove_keys:
                team.pop(key, None)
            teams_list.append(team)
        return teams_list        


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