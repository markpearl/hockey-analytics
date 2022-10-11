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

class GamesData(DataEngineering):
    """[Class for data engineering operations required for ercot weather data from wsi]
    """
    def __init__(self, target_database:str, sql_client:object):
        self.target_database = target_database
        self.target_table = f'games'
        self.sql_client = sql_client

    def extract_data(self):
        """Create data from api call to NHL teams endpoint
        """
        #Iterate through all of the games and perist to sql
        games_list = []
        date_range = pd.date_range(start='2001-08-01', end='2023-08-01',periods=23)
        for start_date, end_date in zip(date_range, date_range[1:]):
            start_dt, end_dt = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
            logger.info(f'Now extracting games between {start_dt} and {end_dt}')
            #Call schedule API
            schedule_output = requests.get(f"{Config.API_URL.value}/schedule?&startDate={start_dt}&endDate={end_dt}").json()
            for date_dict in schedule_output['dates']:
                for game in date_dict['games']:
                    if game['gameType'] not in ['A']: 
                        game['seasonId']=int(game['season'])
                        game['awayTeamId']=game['teams']['away']['team']['id']
                        game['awayTeamGoals']=game['teams']['away']['score']
                        game['awayTeamWin']=game['teams']['away']['leagueRecord']['wins']
                        game['awayTeamLoss']=game['teams']['away']['leagueRecord']['losses']
                        if 'ties' in game['teams']['away']['leagueRecord'].keys():
                            game['awayTeamTies']=game['teams']['away']['leagueRecord']['ties']
                        else:
                            game['awayTeamTies']=None
                        if 'ot' in game['teams']['away']['leagueRecord'].keys():
                            game['awayTeamOTWin']=game['teams']['away']['leagueRecord']['ot']
                        else:
                            game['awayTeamOTWin']=None
                        game['homeTeamId']=game['teams']['home']['team']['id']
                        game['homeTeamGoals']=game['teams']['home']['score']
                        game['homeTeamWin']=game['teams']['home']['leagueRecord']['wins']
                        game['homeTeamLoss']=game['teams']['home']['leagueRecord']['losses']
                        if 'ties' in game['teams']['away']['leagueRecord'].keys():
                            game['homeTeamTies']=game['teams']['home']['leagueRecord']['ties']
                        else:
                            game['awayTeamTies']=None                       
                        if 'ot' in game['teams']['home']['leagueRecord'].keys():
                            game['homeTeamOTWin']=game['teams']['home']['leagueRecord']['ot']
                        else:
                            game['homeTeamOTWin']=None
                        remove_keys = ['link','status','venue','content','teams']
                        for key in remove_keys:
                            game.pop(key, None)
                        games_list.append(game)
        return games_list



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