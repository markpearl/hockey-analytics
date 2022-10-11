import datetime
import logging
from data_eng.src._base import DataEngineering
from base import Config
import requests
import pandas as pd
import requests
import json

logger = logging.getLogger(__name__)

NUMBER_OF_SEASONS=10

class TeamRosters(DataEngineering):
    """[Class for data engineering operations required for ercot weather data from wsi]
    """
    def __init__(self, target_database:str, sql_client:object):
        self.target_database = target_database
        self.target_table = f'team_rosters'
        self.sql_client = sql_client

    def extract_data(self):
        """Create data from api call to NHL team rosters endpoint 
        """        
        team_roster_list = []
        season_response = requests.get(f"{Config.API_URL.value}/seasons").json()
        season_ids = sorted([season['seasonId'] for season in season_response['seasons']],reverse=True)
        team_ids =[team['id'] for team in  requests.get(f"{Config.API_URL.value}/teams").json()['teams']]
        for season_id in season_ids[0:NUMBER_OF_SEASONS]:
            for team_id in team_ids:
                response = requests.get(f"{Config.API_URL.value}/teams/{team_id}/?expand=team.roster&season={season_id}").json()
                if 'messageNumber' not in response.keys():
                    for person in response['teams'][0]['roster']['roster']:
                        roster_dict = {}
                        roster_dict['seasonId']=season_id
                        roster_dict['teamId']=team_id   
                        roster_dict['playerId']=person['person']['id'] 
                        team_roster_list.append(roster_dict)

        return team_roster_list 


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