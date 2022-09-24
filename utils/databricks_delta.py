from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, IntegerType, ArrayType, StructType
from datetime import datetime
import time
import logging
import traceback
from enum import Enum
import uuid

logger = logging.getLogger(__name__)

class DatabricksDelta:

    def __init__(self, env:str, dbutils: object, client: object):
        """[Constructor for curate csv to databricks delta. Instantiates the appropriate databricks and curation
        libraries to load the curated escher csv files into the corresponding databricks delta table.]
        
        Arguments:
            dbutils {object} -- [Represents databricks utilities library for pyspark]
            client {object} -- [Represents the azure key vault client used to retrieve secrets]
        """ 
        self.spark = SparkSession.builder.getOrCreate()
        self.sc = self.spark.sparkContext
        self.dbutils = dbutils
        self.datalake_account_name = f'datalakecentral{env}'

        if client is None:
            self.account_url = dbutils.secrets.get(scope=f"akv-infodel-{env}",key='datalake-url')
            self.account_key = dbutils.secrets.get(scope=f"akv-infodel-{env}",key='blobservice-access-key')
        else: 
            self.account_url = client.get_secret('datalake-url').value
            self.account_key = client.get_secret('blobservice-access-key').value
        self.spark.conf.set("fs.azure.account.key.{0}.dfs.core.windows.net".format(self.datalake_account_name),self.account_key)
        self.spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

    def create_dataframe_from_sql(self, sql_file_location: str):
        """[Creates the required input dataframes for the spark-sql operations to form the target SQL dataframe]
        
        Arguments:
            sql_file_location {str} -- [Location of the path to the .SQL script]
        
        Returns:
            df -- [Corresponding spark dataframe sourced from the .sql script]
        """
        try:        
            #Instantiate the spark dataframe
            sql_query = open(sql_file_location)
            df = self.spark.sql(sql_query.read())
            return df

        except Exception as e:
            logger.error("Error instantiating the input dataframes required to load data to SQL")
            raise(e)
    


    def generate_table_ddl(self, dataframe: DataFrame, delta_table: str, using_option: str, partition_col: str, location: str, include_load_date: bool):
        """[Generates table DDL for create statement of a spark sql table based on columns and datatypes of an input dataframe]

        Arguments:
            dataframe {object} -- [pyspark.sql.dataframe passed to retreive columns and corresponding datatypes]
            delta_table: [target table to be created]
            using_option {str} -- [option to determine file format to be used when writing the table, delta the default.]
            partition_col {str} -- [partition column(s) to use in the target table]
            location {str} -- [location to write the table. This will create an unmanaged/external table]
            include_load_date {bool} -- [boolean to determine if the load_date should be included as a column in the dataframe]
        """
        #Traverse through passed dataframe and create DDL String containing the column names and data type
        dataframe_datatypes = dict(dataframe.dtypes)
        col_def = ""
        
        for idx, column in enumerate(dataframe_datatypes.items()):
            column_name, column_type = column[0], column[1]
            if '@' in column_name or '#' in column_name:
                dataframe = dataframe.withColumnRenamed(column_name,column_name.replace('@','').replace('#',''))
                column_name = column_name.replace('@','').replace('#','')
            col_def = col_def + r"{0} {1}".format(column_name, column_type)
            if idx != len(dataframe_datatypes)-1:
                col_def = col_def + ", "
        
        #Add load_date column to the end of the ddl statement and instantiate it in the dataframe
        if include_load_date:
            dataframe = dataframe.withColumn("load_date",F.current_timestamp())
            col_def = col_def + ", load_date timestamp"

        #Create ddl containing containing delta_table, column definitions and using clause
        ddl = r"CREATE TABLE IF NOT EXISTS {0} ({1}) USING {2} ".format(delta_table,col_def,using_option)
        
        #Add partitioned by clause to ddl if specified/required 
        if partition_col is not None and partition_col in col_def:
            if len(partition_col.split(',')) > 1:
                partition_col = ','.join(partition_col.split(','))
            ddl = ddl + r"PARTITIONED BY ({0}) ".format(partition_col)
        
        #Add location clause to ddl
        if location is not None:
            ddl = ddl + r"LOCATION '{0}'".format(location)

        self.spark.sql('{0}'.format(ddl))
        return dataframe

    def initial_overwrite_load(self, dataframe: DataFrame, write_mode: str, partition_col:str, location: str):
        """[Takes data containing an initial load and inserts the data into 
        the target delta table]

        Arguments:
            dataframe {DataFrame} -- [Pyspark dataframe containing the data]
            write_mode {str} -- [Write mode to use: append, overwrite, etc.]
            partition_col {str} -- [partition column(s) to use in the target table]
            location {str} (optional) -- [Delta lake directory location or table_name if saveAsTable is used]
        """
        #Write the contents of the dataframe to the delta lake location and replace the schema
        if partition_col is None:
            df = dataframe.write.format('delta').mode(write_mode)
        else: 
            df = dataframe.write.format('delta').partitionBy(partition_col).mode(write_mode)
        #Determine if the merge or overwrite schema should be used
        df = df.option("overwriteSchema","True")
        
        #Determine if the dataframe is to be saved to an unmanaged table or managed table 
        if '/' in location:
            return df.save(location)
        else: 
            return df.saveAsTable(location)

    def merge_data(self, dataframe: DataFrame, delta_table: str, merge_columns: list, upsert_condition: str):
        """[Takes data containing an incremental load and merges the data into 
        the target delta table]

        Arguments:
            dataframe {DataFrame} -- [Pyspark dataframe containing the data]
            delta_table {str} -- [Named of target delta table to be merged]
            merge_columns {list} -- [Columns used in merge logic]
            match_conditions {str} -- [Conditions to be used when dealing with updates, inserts, etc.]
        """
        #Create temp view alias so the dataframe can be aliased within sql statement    
        dataframe.createOrReplaceTempView("updates")

        #If merge_columns is not None then iterate through the list and create the merge condition string
        if merge_columns:
            merge_condition = "ON "
            if len(merge_columns) > 1:
                for column in merge_columns[:-1]:
                    merge_condition = merge_condition + '{0}.{1} = updates.{1} AND '.format(delta_table,column)

            merge_condition = merge_condition + '{0}.{1} = updates.{1}'.format(delta_table,merge_columns[len(merge_columns)-1])

        return self.spark.sql("MERGE INTO {0} USING updates {1} {2}".format(delta_table, merge_condition, upsert_condition))

    def read(self, query: str, source: str, source_type: str='DELTA'):
        """[Method used to init]
        
        Arguments:
            query {str} -- [Query executed against sql to populate the dataframe result]
            source {str} -- [Table or entity name to be used as part of the read operation]
            source_type {str} -- [Source type used to run the query against. (i.e. Azure SQL, Synapse, Blob, Datalake, etc.)]
        
        Returns:
            spark_df -- [Return spark dataframe for result set]
        """
        if source_type in 'PARQUET':
            #Read result of the dataframe from Parquet folder location
            spark_df = self.spark.read.parquet(source)
        
        elif source_type in 'CSV':
            spark_df = self.spark.read.csv(source,sep=',',escape="",header=True)

        elif source_type in 'DBFS':
            #Reading dataframe from delta/dbfs location
            spark_df = self.spark.read.format('delta').load(source) 

        elif source_type in 'DELTA':
            #Reading dataframe from databricks delta table
            spark_df = self.spark.sql(query)
            
        else:
            raise NotImplementedError('This source type is currently not supported')
        return spark_df

    def write(self, spark_df: object, write_mode: str, connection_details: str, target: str, target_type: str,truncate: bool):
        """[Method to write the results of a dataframe to the given target type defined by the method]
        
        Arguments:
            spark_df {DataFrame} -- [Input spark dataframe]
            write_mode {str} -- [Mode to write i.e. append, overwrite, etc.]
            connection_details {str} -- [Connection string excluding the username and password]
            target {str} -- [Table name or destination to write]
            target_type {str} -- [Target type the dataframe is written to (i.e. Azure SQL, Synapse, Blob, Datalake, etc.)]
            truncate {bool} -- [Boolean variable used to determine if the truncate option is set to True or False]
        """
        #Determine the target type to write the dataframe to
        if target_type in 'AZURE_SQL':
            #Writing dataframe to Azure SQL database location
            spark_df.write.mode(write_mode) \
                .format("jdbc") \
                .option("truncate",truncate) \
                .option("url", connection_details) \
                .option("batchsize",100000) \
                .option("numPartitions",5) \
                .option("dbtable", target) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .save()

        elif target_type in 'PARQUET':
            #Writing dataframe to parquet location
            spark_df.write.mode("overwrite").parquet(target)

        elif target_type in 'DBFS':
            #Writing dataframe to delta/dbfs location
            spark_df.write.format('delta').save(target) 

        elif target_type in 'DELTA':
            #Reading dataframe from databricks delta table
            self.initial_overwrite_load(spark_df,'DELTA','overwrite',None,target)
        else:
            raise NotImplementedError('This target type is currently not supported')

    def harmonize_schemas_and_combine(self, df_left: DataFrame, df_right: DataFrame):
        """[Looks at the distinct datatype and name differences for columns from two dataframes, harmonizes them
        to make sure the schemas match and unions the dataframes together]
        
        Arguments:
            df_left {DataFrame} -- [Left dataframe used to harmonize schema and union]
            df_right {DataFrame} -- [Right dataframe used to harmonize schema and union]
        
        Returns:
            union_df [DataFrame] -- [Unioned dataframe with harmonized schema]
        """
        #Remove any special characters from existing dataframe schemas
        df_left = self.remove_special_chars(df_left)
        df_right = self.remove_special_chars(df_right)

        #Collect the data types and column names for the two passed dataframes
        left_types = {f.name: f.dataType for f in df_left.schema}
        right_types = {f.name: f.dataType for f in df_right.schema}
        left_fields = set((f.name, f.dataType) for f in df_left.schema) #f.nullable) for f in df_left.schema)
        right_fields = set((f.name, f.dataType) for f in df_right.schema) #f.nullable)

        #Determine the unique fileds across both dataframes and create common schema structure between both dataframes
        df_result_left, df_unique_left_elems = self._determine_unique_fields(right_fields,left_fields,right_types,left_types, df_left)
        df_result_right, df_unique_right_elems = self._determine_unique_fields(left_fields,right_fields,left_types,right_types, df_right)
        logger.info("There are: {0} unique columns from the right_dataframe that are not in left_dataframe and they are: {1}".format(str(len(df_unique_left_elems)),df_unique_left_elems))
        logger.info("There are: {0} unique columns from the left_dataframe that are not in the right_dataframe and they are: {1}".format(str(len(df_unique_right_elems)),df_unique_right_elems))
        
        #Make sure columns are in the same order
        logger.info("Performing union on resulting left and right dataframes")
        df_result_left = df_result_left.select(df_result_right.columns)

        #Union the dataframes together
        unioned_df = df_result_left.union(df_result_right)
        return unioned_df

    def remove_special_chars(self, dataframe: DataFrame):
        """[This method iterates through a dataframes and returns the dataframe which special characters stripped out]
        
        Arguments:
            dataframe {DataFrame} -- [Pyspark dataframe containing the data]
        """        
        #Remove any special characters from existing dataframe schemas
        for column, datatype in dataframe.dtypes:
            if '@' in column or '#' in column:
                dataframe = dataframe.withColumnRenamed(column,column.replace('@','').replace('#',''))
                column = column.replace('@','').replace('#','')
        return dataframe

    def _determine_unique_fields(self, df1_fields: set, df2_fields: set, df1_types: dict, df2_types: dict, df2_dataframe: DataFrame):
        """[Determine the unique fields across two dataframes based on the field name and type]
        
        Arguments:
            df1_fields {set} -- [Column names from datframe1]
            df2_fields {set} -- [Column names from datframe2]
            df1_types {dict} -- [Column types from datframe1]
            df2_types {dict} -- [Column types from datframe2]
            df2_dataframe {DataFrame} -- [Returned dataframe containing the union a common schema for both passed dataframes]
        
        Raises:
            TypeError: [Union failed. Type conflict on two fiels from the first and second dataframe"]
        
        Returns:
            [dataframe, field_difference] -- [Returns the resulting dataframe and the field difference]
        """
        logger.info("Determine difference between df1_fields and df2_fields")
        #Use set logic to determine the field difference between dataframe1 and dataframe2 fields
        field_difference = df1_fields.difference(df2_fields)
        for df1_name, df1_type in field_difference:
            #Check if the dataframe1 column is not in the dataframe2 dictionary 
            if df1_name in df2_types:
                df2_type = df2_types[df1_name]
                if df1_type != df2_type:
                    raise TypeError("Union failed. Type conflict on field %s. right type %s, left type %s" % (df1_name, df1_type, df2_type))
            df2_dataframe = df2_dataframe.withColumn(df1_name, F.lit(None).cast(df1_type))
        return df2_dataframe, field_difference