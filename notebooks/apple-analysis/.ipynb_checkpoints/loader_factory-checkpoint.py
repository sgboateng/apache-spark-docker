# Abstract loader class
class Loader:
    
    # Abstract class
    def __init__(self, dataframe, path, method, params):
        self.dataframe = dataframe
        self.path = path
        self.method = method
        self.params = params

    # Abstract method, function will be defined in subclasses
    def load_data_frame(self):
        raise ValueError("Not Implemented")

class LoadToCSV(Loader):

    def load_data_frame(self):
        
        self.dataframe \
            .write.option("header",True) \
                    .format("csv") \
                        .mode(self.method) \
                            .save(self.path)

class LoadToParquetWithPartitions(Loader):

    def load_data_frame(self):

        partitionByColumns = self.params.get("partition_by_columns")
        
        self.dataframe \
            .write \
                .format("parquet") \
                    .partitionBy(*partitionByColumns) \
                        .mode(self.method) \
                            .save(self.path)

# Function to get data sink source
def get_sink_source(sink_type, dataframe, path, method, params=None):

    if sink_type == "csv":
        return LoadToCSV(dataframe, path, method, params)
        
    elif sink_type == "parquet_with_partitions":
        return  LoadToParquetWithPartitions(dataframe, path, method, params)
        
    else:
        raise ValueError(f"Not implemented for sink_type: {sink_type}")
        