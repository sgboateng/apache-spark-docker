# Import Libraries
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains, explode

# Abstract transform class
class Transformer:
    
    # Abstract class
    def __init__(self):
        pass

    # Abstract method, function will be defined in subclasses
    def transform(self, input_DFs):
        pass

class AirpodsAfterIphoneTransformer(Transformer):

    # Customer who have bought Airpods after buying iPhone
    def transform(self, input_DFs):
        
        # Extract Transactions data from dictionary
        transactions_input_DFs = input_DFs.get("lbl_transactions_DFs")

        # Extract Customers data from dictionary
        customers_input_DFs = input_DFs.get("lbl_customers_DFs")
        
        # Partition data frame using Window function
        window_specification = Window.partitionBy("customer_id").orderBy("transaction_date")

        # Create new column representing "next_product_name"
        transactions_input_DFs_window = transactions_input_DFs.withColumn(
            "next_product_name", lead("product_name").over(window_specification)
        )

        # Filter out all required rows
        transactions_input_DFs_filtered = transactions_input_DFs_window.filter(
            (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods")
        )

        # Customers who purchased Airpods after purchasing Iphones"
        joined_input_DFS = customers_input_DFs.join(
            broadcast(transactions_input_DFs_filtered), 
                "customer_id")

        return joined_input_DFS

class OnlyAirpodsAndIphoneTransformer(Transformer):

    # Customer who have bought only iPhone and Airpods nothing else
    def transform(self, input_DFs):

        # Extract Transactions data from dictionary
        transactions_input_DFs = input_DFs.get("lbl_transactions_DFs")

        # Group Transactions data by customer_id
        transactions_input_grouped_DFs = transactions_input_DFs.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )

        # Filter out all required rows
        transactions_input_DFs_filtered = transactions_input_grouped_DFs.filter(
            (array_contains(col("products"), "iPhone")) & (array_contains(col("products"), "AirPods")) & 
                (size(col("products")) == 2)
                    )

        # Extract Customers data from dictionary
        customers_input_DFs = input_DFs.get("lbl_customers_DFs")

        # Customers who purchased Airpods after purchasing Iphones"
        joined_input_DFS = customers_input_DFs.join(
            broadcast(transactions_input_DFs_filtered), 
                "customer_id")
        """
        joined_input_DFS_exploded = joined_input_DFS \
            .withColumn("product_name", explode("products")) \
                .drop("products")
        """
        return joined_input_DFS