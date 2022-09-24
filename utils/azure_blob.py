import logging
from azure.storage.blob import BlobServiceClient, BlobClient
import json
from pandas import DataFrame
import os 

logger = logging.getLogger(__name__)

class AzureBlob(object):
    
    def __init__(self, dbutils: object, client: object, environment: str):
        """
        Constructor for Azure Blob class, which instantiates a connection to an
        Azure storage account and exposes methods to download and manipulate blobs.
        Also compatible with Azure Data Lake Gen2

        Arguments:
            dbutils {object} -- [Represents databricks utilities library for pyspark]
            client {object} -- [Represents the azure key vault client used to retrieve secrets]
            environment {str} -- [Represents the current environment (i.e. dev, qa, prod)]
        """
        if client is None:
            self.sas_url = dbutils.secrets.get(scope=f"akv-infodel-{environment}", key="blobservice-sas-url-analysts")
        else:
            self.sas_url = client.get_secret("blobservice-sas-url").value
        self.blob_service = BlobServiceClient.from_connection_string(self.sas_url)

    def delete_blob(self, container_name: str, blob_name: list):
        """
        Lists all of the blobs present in a given container or folder of a container denoted by the prefix 

        Arguments:
            container_name {str} -- Container name within the azure storage account to be connected to
            blob_name {list} -- Blob name(s) to be deleted from the container 
        """
        try:
            container_client = self.blob_service.get_container_client(container_name)
            logger.info('Deleting blob from given container: {0} and prefix: {1} of the blob name'.format(container_name, blob_name))
            container_client.delete_blobs(*blob_name)
        except Exception as e:
            logger.error(str(e))
            raise(e)        

    def get_blob_list(self, container_name: str, prefix: str):
        """
        Lists all of the blobs present in a given container or folder of a container denoted by the prefix 

        Arguments:
            container_name {str} -- Container name within the azure storage account to be connected to
            prefix {str} -- Prefix for the blob name in case we're connecting to blob(s) within a given folder of a container
        """
        try:
            container_client = self.blob_service.get_container_client(container_name)
            logger.info('Listing the blobs from given container: {0} and prefix: {1} of the blob name'.format(container_name, prefix))
            blob_list = [blob.name for blob in list(container_client.list_blobs(name_starts_with=prefix)) if blob.size > 0]
            if blob_list is not None:                
                return blob_list
        except Exception as e:
            logger.error(str(e))
            raise(e)

    def get_blob_subdirs(self, container_name: str, parent_folder:str, delim: str):
        """
        Lists all of the blobs present in a given container or folder of a container denoted by the prefix 

        Arguments:
            container_name {str} -- Container name within the azure storage account to be connected to
            parent_folder {str} -- Parent folder to list the sub directories
            delim {str} -- Delimiter used for walk blobs function
        """
        try:
            container_client = self.blob_service.get_container_client(container_name)
            logger.info('Listing the sub directories from given container: {0} and folder: {1}'.format(container_name, parent_folder))
            blob_list = [blob.name for blob in list(container_client.walk_blobs(parent_folder,delimiter=delim))]
            if blob_list is not None:                
                return blob_list
        except Exception as e:
            logger.error(str(e))
            raise(e)            

    def read(self, container_name: str, filename: str):
        """

        Arguments:
            container_name {str} -- Container name within the azure storage account to be connected to
            prefix {str} -- Prefix for the blob name in case we're connecting to blob(s) within a given folder of a container
        """
        try:
            logger.info('Loading the blob from given container: {0} and name: {1} to text'.format(container_name, filename))
            blob = BlobClient.from_connection_string(conn_str=self.sas_url, container_name=container_name, blob_name=filename)
            stream = blob.download_blob()
            data = stream.readall().decode("utf-8")
            if data is not None:
                return data
        except Exception as e:
            logger.error(str(e))
            raise(e)

    def write(self, file_path:str, data: DataFrame, target_container_name: str):
        """[Method to load the file from memory to the datalake location]
        
        Arguments:
            file_path {str} -- [File path for azure data lake where data will be loaded]
            data {DataFrame} -- [Data from pandas dataframe to be loaded]
            target_container_name {str} -- [Container the file will be loaded to]
        """        
        try:
            logger.info('Now loading {0} into blob as part of '.format(file_path))
            container_client = self.blob_service.get_container_client(target_container_name)
            container_client.upload_blob(name=file_path, data=data)
        except Exception as e:
            logger.error(str(e))
            raise(e)