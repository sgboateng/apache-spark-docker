# Import Classes & Methods
from loader_factory import get_sink_source

class AbstractLoader:
    def __init__(self, transformed_DFs):
        self.transformed_DFs = transformed_DFs

    def sink(self):
        pass

class AirPodsAfterIphoneLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type = "csv", 
            dataframe = self.transformed_DFs, 
            path = "../spark-data/output/", 
            method = "overwrite"
        ).load_data_frame()

class OnlyAirpodsAndIPhoneLoader(AbstractLoader):

    def sink(self):
        params = {
            "partition_by_columns": ["location"]
        }
        
        get_sink_source(
            sink_type = "parquet_with_partitions", 
            dataframe = self.transformed_DFs, 
            path = "../spark-data/output/", 
            method = "overwrite", 
            params = params
        ).load_data_frame()