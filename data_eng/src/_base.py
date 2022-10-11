from abc import ABCMeta, abstractmethod
#from pyspark.sql import DataFrame
import re 

class DataEngineering(metaclass=ABCMeta):

    @abstractmethod
    def extract_data(self):
        pass
    
    @abstractmethod
    def transform_data(self):
        pass
    
    @abstractmethod
    def load_data(self):
        pass    


    @staticmethod
    def rename_columns(input_df: object, rename_list:list, col_suffix:str, remove_numbers:bool):
        """Rename column in the list and append suffix to end of column name

        Args:
            input_df (DataFrame): [Input dataframe with columns to be renamed]
            rename_list (list): [Names of columns to rename]
            col_suffix (str): [Suffix to use for renamed column]
        """        
        #Iterate through each element (i.e. [NORTH (LOAD), becomes NORTH_LOAD ])
        for col in rename_list:
            if remove_numbers:
                input_df = input_df.withColumnRenamed(col, "_".join(re.findall("[a-zA-Z]+", col))+col_suffix)
            else:
                input_df = input_df.withColumnRenamed(col, "_".join(re.findall("[a-zA-Z0-9]+", col))+col_suffix)
        return input_df