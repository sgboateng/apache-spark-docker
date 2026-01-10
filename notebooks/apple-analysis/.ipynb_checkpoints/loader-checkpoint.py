# Import Classes & Methods
from loader_factory import get_sink_source

class AbstractLoader:
    def __init__(self, transformed_DFs):
        self.transformed_DFs = transformed_DFs

    def sink(self):
        pass

class AirPodsAfterIphoneLoader(AbstractLoader):

    def sink(self):
        # Create root directory
        OUTPUT_DATA_ROOT = "/opt/spark-data/output"
        
        get_sink_source(
            sink_type = "csv", 
            dataframe = self.transformed_DFs, 
            path = f"{OUTPUT_DATA_ROOT}/apple-analysis/", 
            method = "overwrite"
        ).load_data_frame()

class OnlyAirpodsAndIPhoneLoader(AbstractLoader):

    def sink(self):
        # Create root directory
        OUTPUT_DATA_ROOT = "/opt/spark-data/output"
        
        params = {
            "partition_by_columns": ["location"]
        }
        
        get_sink_source(
            sink_type = "parquet_with_partitions", 
            dataframe = self.transformed_DFs, 
            path = f"{OUTPUT_DATA_ROOT}/apple-analysis/", 
            method = "overwrite", 
            params = params
        ).load_data_frame()