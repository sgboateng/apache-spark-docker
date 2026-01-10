# Import Classes & Methods
from reader_factory import get_data_source

# Abstract extractor class
class Extractor:
    
    # Abstract class
    def __init__(self):
        pass
        
    # Abstract method, function will be defined in subclasses
    def extract(self):
        pass

class CustomerTransactionsExtractor(Extractor):

    def extract(self):
        # Create root directory
        INPUT_DATA_ROOT = "/opt/spark-data/input"
        
        # Implement the steps for extracting or reading the data
        transactions_DFs = get_data_source(
            data_type="csv", 
            file_path = f"{INPUT_DATA_ROOT}/apple-analysis/Transaction_Updated.csv"
        ).get_data_frame()

        customers_DFs = get_data_source(
            data_type="csv", 
            file_path=f"{INPUT_DATA_ROOT}/apple-analysis/Customer_Updated.csv"
        ).get_data_frame()
        
        # Create input_DFs
        input_DFs = {
            "lbl_transactions_DFs":transactions_DFs, 
            "lbl_customers_DFs":customers_DFs
        }
        
        return input_DFs