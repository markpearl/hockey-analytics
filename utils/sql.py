from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import urllib
import pyodbc
from pandas import DataFrame

class SQL:
    def __init__(self, database):
        """Class for SQL operations

        Args:
            database (_type_): Name of databsae to connect to
        """        
        conn_str = (
            r'DRIVER={SQL Server};'
            r'SERVER=(local)\SQLEXPRESS;'
            f'DATABASE={database};'
            r'Trusted_Connection=yes;'
        )
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
        self.engine = create_engine(connection_url)


    def write_data(self, df:DataFrame, target_table:str):
        """Write data using connection

        Args:
            df (DataFrame): _description_
            target_table (str): _description_
        """              
        df.to_sql(target_table, schema='dbo', con = self.engine, chunksize=10, method='multi', index=False, if_exists='replace')

