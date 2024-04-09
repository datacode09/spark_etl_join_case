from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import coalesce, lit, concat, concat_ws, col, trim, upper, regexp_replace, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_spark_session(app_name="ETLFramework"):
    return SparkSession.builder.appName(app_name).getOrCreate()

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, trim, upper, concat_ws, col, when, lit
from pyspark.sql.types import StructType
import os

def extract_incoming_vol_data(spark: SparkSession, source, expected_schema: StructType = None) -> DataFrame:
    """
    Extracts data from a given source, which can be a path to a .parquet file, a directory containing .parquet files,
    or a DataFrame object. Then, applies the incoming_vol_join_key_logic to add a JoinKey.

    Parameters:
    - spark: The SparkSession object.
    - source: The data source, which can be a path (str) or a DataFrame.
    - expected_schema: The expected schema of the DataFrame, applicable if the source is a path.

    Returns:
    - A DataFrame with the added 'JoinKey' column.
    """
    # Handle different types of input sources
    if isinstance(source, str):
        if source.endswith('.parquet'):
            df = spark.read.parquet(source)
        elif os.path.isdir(source):
            df = spark.read.parquet(f"{source}/*.parquet")
        else:
            # Assuming CSV as a default if not specified as parquet but adjust as needed
            df = spark.read.schema(expected_schema).csv(source, header=True, inferSchema=True) if expected_schema else spark.read.csv(source, header=True, inferSchema=True)
    elif isinstance(source, DataFrame):
        df = source
    else:
        raise ValueError("Source must be a path (str) or a DataFrame.")
    
    # Apply join key logic if DataFrame has been successfully loaded
    if df is not None:
        df = incoming_vol_join_key_logic(df)

    return df

def incoming_vol_join_key_logic(df: DataFrame) -> DataFrame:
    # Logic remains the same as previously defined
    columns_to_transform = [
        "data_source", "process", "subprocess_1", "subprocess_2",
        "subprocess_3", "subprocess_5", "subprocess_6"
    ]
    def transform_column(col_name):
        return trim(upper(regexp_replace(col(col_name), "\\W", "")))
    df = df.withColumn(
        "JoinKey",
        when(col("data_source") == "CART",
             concat_ws("|",
                       *[transform_column(c) for c in columns_to_transform],
                       lit(""),  # Placeholder for subprocess_4
                       transform_column("subprocess_6"), lit("|"))
        ).otherwise(
            concat_ws("|",
                      *[transform_column(c) for c in columns_to_transform[:4]],
                      transform_column("subprocess_4"),
                      *[transform_column(c) for c in columns_to_transform[4:]],
                      lit("|"))
        )
    )
    return df


def extract_activity_list_data(spark, nas_path: str) -> DataFrame:
    try:
        return spark.read.csv(nas_path, header=True, inferSchema=True)
    except Exception as e:
        logging.error(f"Failed to extract data from NAS at {nas_path}: {e}")
        raise

def extract_emp_hierarchy_data(spark) -> DataFrame:
    try:
        hive_table = "prod_rie0_atom.enterprisehierarchy"
        sql_query = f"SELECT employeeid, employeename, MAX(snap_date) AS latest_snap_date FROM {hive_table} GROUP BY employeeid, employeename"
        return spark.sql(sql_query)
    except Exception as e:
        logging.error(f"Failed to extract data from Hive table {hive_table}: {e}")
        raise

def identify_join_columns(df1: DataFrame, df2: DataFrame) -> list:
    df1_columns = set(df1.columns)
    df2_columns = set(df2.columns)
    return list(df1_columns.intersection(df2_columns))

def join_step_2_and_3_data(step_2_output: DataFrame, step_3_output: DataFrame) -> DataFrame:
    join_columns = identify_join_columns(step_2_output, step_3_output)
    if not join_columns:
        raise ValueError("No suitable join columns were identified between the two DataFrames.")
    return step_2_output.join(step_3_output, join_columns, 'outer')

def join_and_save_data(spark, primary_df, activity_list_df, emp_hierarchy_df, join_columns, intermediate_output_paths=None, final_output_path=None):
    step_2_output = primary_df.join(activity_list_df, join_columns['primary_activity'], "left").select(primary_df["*"], coalesce(activity_list_df["New_Center"], lit("Not Defined")).alias("Center"), coalesce(activity_list_df["Capacity_Planning_Group"], lit("Not Defined")).alias("Capacity_Planning_Group"))
    step_3_output = primary_df.join(emp_hierarchy_df, join_columns['primary_emp_hierarchy'])

    final_output = join_step_2_and_3_data(step_2_output, step_3_output)

    if final_output_path:
        final_output.write.mode("overwrite").parquet(final_output_path)
    else:
        return final_output
