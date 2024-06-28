import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, to_json, expr

def load_hsbc_client_list(spark, data):
    columns = ["clnt_no", "clnt_spcfc_cd_val"]
    return spark.createDataFrame(data, columns)

def hsbc_indicator_enrichment(spark, client_list_data, data):
    # Load the hsbc_client_list data into a DataFrame
    hsbc_client_list = load_hsbc_client_list(spark, client_list_data)

    # Original DataFrame with client_number and additional_features
    columns = ["id", "additional_features", "client_number"]
    df = spark.createDataFrame(data, columns)

    # Create a temporary column hsbc_indicator in the original DataFrame
    df = df.join(hsbc_client_list, df.client_number == hsbc_client_list.clnt_no, "left_outer") \
           .withColumn("hsbc_indicator", when(col("clnt_spcfc_cd_val").isNotNull(), col("clnt_spcfc_cd_val")).otherwise("N")) \
           .drop("clnt_no", "clnt_spcfc_cd_val")

    # Use built-in functions to add hsbc_indicator to additional_features JSON field
        # Use built-in functions to add hsbc_indicator to additional_features JSON field
    df = df.withColumn("additional_features", 
                       when(col("additional_features").isNotNull(), 
                            expr("""
                                 to_json(
                                     map_concat(
                                         from_json(additional_features, 'map<string,string>'), 
                                         map('hsbc_indicator', hsbc_indicator)
                                     )
                                 )
                                 """)
                           ).otherwise(to_json(expr("map('hsbc_indicator', hsbc_indicator)"))))
    
    return df

# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName("HSBC Enrichment Test").getOrCreate()
    
    # Sample client list data
    client_list_data = [
        (123, 'A'),
        (456, 'B')
    ]

    # Sample data
    data = [
        (1, '{"key1": "value1"}', 123),
        (2, '{"key2": "value2"}', 456),
        (3, None, 789)
    ]
    
    # Run the enrichment
    enriched_df = hsbc_indicator_enrichment(spark, client_list_data, data)
    enriched_df.show(truncate=False)

    # Stop the Spark session
    spark.stop()
