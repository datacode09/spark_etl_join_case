import logging
from pyspark.sql import SparkSession, DataFrame

def enrich_primary_with_activity_data(primary_df: DataFrame, activity_df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Enrich the primary DataFrame with additional data from the activity DataFrame using Spark SQL.

    Parameters:
    - primary_df (DataFrame): The primary Spark DataFrame to be enriched.
    - activity_df (DataFrame): The activity DataFrame containing additional data.
    - spark (SparkSession): The active SparkSession instance.

    Returns:
    - DataFrame: The enriched primary DataFrame.
    """
    logging.info("Enriching primary DataFrame with activity list data.")
    try:
        # Register the DataFrames as temporary views
        primary_df.createOrReplaceTempView("primary_table")
        activity_df.createOrReplaceTempView("activity_table")

        # Define the SQL query for enrichment
        query = """
        SELECT p.*, 
               COALESCE(a.New_Center, 'Not Defined') AS Center,
               COALESCE(a.Capacity_Planning_Group, 'Not Defined') AS Capacity_Planning_Group
        FROM primary_table p
        LEFT OUTER JOIN activity_table a
        ON p.JoinKey = a.JoinKey
        """

        # Execute the SQL query
        enriched_output = spark.sql(query)

        logging.info("Primary data enrichment with activity data completed.")
        return enriched_output

    except Exception as e:
        logging.error(f"Error enriching primary data with activity data: {e}")
        raise
