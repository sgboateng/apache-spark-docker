# Import Libraries
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
            .appName("Apple-Analysis-App") \
                .getOrCreate()

# Abstract data source class
class DataSource:
    
    # Abstract class
    def __init__(self, path):
        self.path = path

    # Abstract method, function will be defined in subclasses
    def get_data_frame(self):
        raise ValueError("Not Implemented")

# CSV data source
class CSVDataSource(DataSource):
    
    def get_data_frame(self):
        return (\
            spark.read.format("csv") \
                .option("header", True) \
                    .load(self.path)
        )

# Parquet data source
class ParquetDataSource(DataSource):
    
    def get_data_frame(self):
        return (\
            spark.read.format("parquet") \
                .load(self.path)
        )

# Delta data source
class DeltaDataSource(DataSource):
    
    def get_data_frame(self):        
        table_name = self.path        
        return (
            spark.read.table(table_name)
        )

# Function to get data source
def get_data_source(data_type, file_path):

    if data_type == "csv":
        return  CSVDataSource(file_path)
        
    elif data_type == "parquet":
        return  ParquetDataSource(file_path)
        
    elif data_type == "delta":
        return  DeltaDataSource(file_path)
        
    else:
        raise ValueError(f"Not implemented for data_type: {data_type}") 