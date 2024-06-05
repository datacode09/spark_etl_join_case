from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sequence, to_date

def calculate_snap_dates(df):
    """
    Calculate the min and max dates from the StartDate and EndDate columns.

    Args:
    df (DataFrame): Spark DataFrame with StartDate and EndDate columns.

    Returns:
    tuple: (min_date, max_date)
    """
    min_date = df.selectExpr("MIN(StartDate) as min_start_date").collect()[0]['min_start_date']
    max_date = df.selectExpr("MAX(EndDate) as max_end_date").collect()[0]['max_end_date']
    return min_date, max_date

def create_snap_date_column(df):
    """
    Create a snap_date column to convert the dataframe into a time series.

    Args:
    df (DataFrame): Spark DataFrame with StartDate and EndDate columns.

    Returns:
    DataFrame: Spark DataFrame with snap_date column.
    """
    df_with_snap_date = df.withColumn("snap_date", explode(sequence(col("StartDate"), col("EndDate"))))
    return df_with_snap_date
