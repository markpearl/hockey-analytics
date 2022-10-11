import datetime
import logging
from re import S
from data_eng.src._base import DataEngineering
from base import Config
import requests
import pandas as pd
import requests
import json

logger = logging.getLogger(__name__)

class GameEventsData(DataEngineering):
    """[Class for data engineering operations required for ercot weather data from wsi]
    """
    def __init__(self, target_database:str, sql_client:object):
        self.target_database = target_database
        self.target_table = f'game_events'
        self.sql_client = sql_client

    def extract_data(self):
        """Create data from api call to NHL teams endpoint
        """
        #Iterate through all of the games and perist to sql
        sql = "select distinct(gamePk) as gamePk from dbo.games WHERE gameDate < CURRENT_TIMESTAMP AND year(gameDate) >= 2021 ORDER BY gamePk DESC"
        game_ids_df = pd.read_sql(sql,con=self.sql_client.engine)
        game_event_list = []
        for game_id in list(game_ids_df['gamePk']):
            logger.info(f'Determining if game events exist in game: {game_id}')
            game_response = requests.get(f"{Config.API_URL.value}/game/{game_id}/feed/live").json()
            if len(game_response['liveData']['plays']['allPlays']) > 1: 
                for play in game_response['liveData']['plays']['allPlays']:
                    if 'players' in list(play.keys()):
                        game_event = {}
                        game_event['gamePk']=game_id
                        game_event.update(play['about'])
                        game_event['homeScore']=game_event['goals']['home']
                        game_event['awayScore']=game_event['goals']['away']
                        game_event.pop('goals')
                        game_event.update(play['result'])
                        game_event.update(play['coordinates'])
                        for player_data in play['players']:
                            player_game_event = game_event.copy()
                            player_game_event['playerId']=player_data['player']['id']
                            player_game_event['playerFullName']=player_data['player']['fullName']
                            player_game_event['playerType']=player_data['playerType']
                            game_event_list.append(player_game_event)


            else:
                logger.info(f"No live game data for game id: {game_id}")

        return game_event_list



    def transform_data(self, input_list: object):
        """Method to perform the required pivots or aggregations on the dataframe before loading

        Args:
            input_lis (list): [Input list containing response from API]
        """
        #Transform the required elements and create transformed dataframe
        df = pd.DataFrame(input_list)
        return df


    def load_data(self, transformed_df: object):
        """Method to load data to its corresponding table in the bronze table
        """
        for idx, row in transformed_df.iterrows():
            logger.info()
            self.sql_client.write_data(transformed_df, self.target_table)