from datetime import timedelta, datetime
from pytz import timezone
from ercot.base import LMPConfig
from pyspark.sql import functions as f
from pyspark.sql import DataFrame

def create_time_horizons(job_step:str, tz:str= 'America/Chicago'):
    """Create time horizons based on 

    Args:
        job_step (str): [Job step that is consuming results of the pipeline (i.e. inference, training, etc.)]
        tz (str): [Corresponding timezone for the model results (i.e. America/Chicago, America/Denver, etc.)]
    """
    
    utc_tz = timezone('UTC')
    local_tz = timezone(tz) # should be function parameter in future 

    dt_local_now = datetime.utcnow().astimezone(local_tz)
    if job_step == 'training':
        dt_horizon_start = datetime.strptime(LMPConfig.HISTORICAL_CUTOFF_DATE.value, "%Y-%m-%d %H:%M:%S").astimezone(local_tz)
        dt_horizon_end = dt_local_now - timedelta(days=16)
    
    elif job_step == 'inference':
        dt_horizon_start = dt_local_now - timedelta(days=15)
        dt_horizon_end = dt_local_now + timedelta(days=7)

    # if entity_name == 'historical_lmp':
    #     dt_horizon_end = dt_local_now + timedelta(hours=1)

    dt_horizon_end = datetime.strptime(datetime.strftime(dt_horizon_end, "%Y-%m-%d 23"), "%Y-%m-%d %H")
    dt_horizon_end_utc = local_tz.localize(dt_horizon_end).astimezone(utc_tz)
    dt_horizon_start = datetime.strptime(datetime.strftime(dt_horizon_start, "%Y-%m-%d 00"), "%Y-%m-%d %H")
    dt_horizon_start_utc = local_tz.localize(dt_horizon_start).astimezone(utc_tz)
    
    return dt_horizon_start_utc, dt_horizon_end_utc    

def filter_horizons(df: DataFrame, dt_horizon_start_utc:datetime, dt_horizon_end_utc:datetime, time_column_name:str):
    """Reduce datetime column by an interval of 1 hour and filter based on
    datetimes

    Args:
        df (DataFrame): [Input dataframe to be filtered]
        dt_horizon_start_utc (datetime): [Start of datetime horizon]
        dt_horizon_end_utc (datetime): [End of datetime horizon]
        time_column_name (str): [Name of time column where the filter will be applied]


    Returns:
        [filtered_df DataFrame]: [Filtered dataframe]
    """        
    filtered_df = df.where(
            (f.col(time_column_name) > dt_horizon_start_utc) &
            (f.col(time_column_name) <= dt_horizon_end_utc)
        )
    return filtered_df


def add_date_features(learning_df: DataFrame,time_column_name:str):
    """Method used to create date based features

    Args:
        learning_df (DataFrame): [Pandas dataframe containing both X and Y before the data is split]
        time_column_name (str): [Time column name to be dropped]
    """
    learning_df = learning_df.withColumn('year', f.year(time_column_name))
    learning_df = learning_df.withColumn('month', f.month(time_column_name))
    learning_df = learning_df.withColumn('hour', f.hour(time_column_name))
    return learning_df

  