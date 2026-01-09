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
        
        # Implement the steps for extracting or reading the data
        transactions_DFs = get_data_source(
            data_type="csv", 
            file_path="../spark-data/input/Transaction_Updated.csv"
        ).get_data_frame()

        customers_DFs = get_data_source(
            data_type="csv", 
            file_path="../spark-data/input/Customer_Updated.csv"
        ).get_data_frame()
        
        # Create input_DFs
        input_DFs = {
            "lbl_transactions_DFs":transactions_DFs, 
            "lbl_customers_DFs":customers_DFs
        }
        
        return input_DFs