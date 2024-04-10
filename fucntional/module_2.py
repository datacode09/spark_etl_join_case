from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import coalesce, lit, concat, concat_ws, col, trim, upper, regexp_replace, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
import logging
import os
from pyspark.sql.functions import get_json_object


# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_spark_session(app_name="ETLFramework"):
    return SparkSession.builder.appName(app_name).getOrCreate()

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

def transform_column(col_name):
    """
    Transforms a column name by trimming, converting to uppercase, and replacing non-word characters.

    Parameters:
    - col_name: The name of the column to transform.

    Returns:
    - The transformed column.
    """
    return trim(upper(regexp_replace(col(col_name), "\\W", "")))
def incoming_vol_join_key_logic(df: DataFrame) -> DataFrame:
    """
    Applies join key logic specific to the incoming volume data.

    Parameters:
    - df: The DataFrame to process.

    Returns:
    - DataFrame with an added 'JoinKey' column.
    """
    columns_to_transform = [
        "data_source", "process", "subprocess_1", "subprocess_2",
        "subprocess_3", "subprocess_5", "subprocess_6"
    ]

    # Use the standalone transform_column function within the DataFrame transformation
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
        
def enrich_primary_with_activity_data(primary_df_with_join_key: DataFrame, activity_list_df: DataFrame) -> DataFrame:
    """
    Enriches the primary DataFrame with join key extracted with additional data from the activity list DataFrame.

    Parameters:
    - primary_df_with_join_key: The primary DataFrame with the join key extracted.
    - activity_list_df: DataFrame containing activity list information.

    Returns:
    - A DataFrame resulting from the enriched primary data with activity-related insights.
    """
    try:
        enriched_output = primary_df_with_join_key.join(activity_list_df, primary_df_with_join_key["JoinKey"] == activity_list_df["JoinKey"], "left_outer").select(
            primary_df_with_join_key["*"],
            coalesce(activity_list_df["New_Center"], lit("Not Defined")).alias("Center"),
            coalesce(activity_list_df["Capacity_Planning_Group"], lit("Not Defined")).alias("Capacity_Planning_Group")
        )
        return enriched_output
    except Exception as e:
        logging.error(f"Error enriching primary data with activity data: {e}")
        raise

from pyspark.sql.functions import get_json_object
def extract_employee_number_from_json(df: DataFrame, json_column_name: str, key_name: str) -> DataFrame:
    """
    Extracts a value from a JSON-encoded column and creates a new column with this value.

    Parameters:
    - df: DataFrame containing the JSON column.
    - json_column_name: The name of the column containing the JSON string.
    - key_name: The key within the JSON from which to extract the value.

    Returns:
    - DataFrame with a new column added for the extracted value.
    """
    json_path = f"$.{key_name}"
    return df.withColumn("employee_number", get_json_object(col(json_column_name), json_path))


def enrich_primary_with_emp_hierarchy(primary_df_with_join_key: DataFrame, emp_hierarchy_df: DataFrame, json_column_name: str) -> DataFrame:
    """
    Enriches the primary DataFrame with join key extracted with employee hierarchy information.

    Parameters:
    - primary_df_with_join_key: The primary DataFrame with the join key extracted.
    - emp_hierarchy_df: DataFrame containing employee hierarchy information.
    - json_column_name: The name of the column in primary_df containing the JSON string with employee information.

    Returns:
    - A DataFrame resulting from the enriched primary data with employee hierarchy insights.
    """
    try:
        # Extract 'employee_number' join key from the JSON column in primary_df
        primary_df_with_employee_number = extract_employee_number_from_json(primary_df_with_join_key, json_column_name, "employee_number")
        
        # Perform the join operation using 'employee_number'
        enriched_output = primary_df_with_employee_number.join(emp_hierarchy_df, primary_df_with_employee_number["employee_number"] == emp_hierarchy_df["employeeid"], "left_outer")
        
        return enriched_output
    except Exception as e:
        logging.error(f"Error enriching primary data with employee hierarchy: {e}")
        raise


def find_common_join_keys(df1: DataFrame, df2: DataFrame) -> list:
    """
    Identifies common join keys between two DataFrames by comparing their column names.

    Parameters:
    - df1: First DataFrame.
    - df2: Second DataFrame.

    Returns:
    - A list of common column names between df1 and df2.
    """
    df1_columns = set(df1.columns)
    df2_columns = set(df2.columns)
    common_columns = list(df1_columns.intersection(df2_columns))
    return common_columns


def merge_enriched_data(enriched_activity_data: DataFrame, enriched_emp_hierarchy_data: DataFrame) -> DataFrame:
    """
    Merges two enriched DataFrames based on common join keys.

    Parameters:
    - enriched_activity_data: DataFrame enriched with activity data.
    - enriched_emp_hierarchy_data: DataFrame enriched with employee hierarchy data.

    Returns:
    - A DataFrame resulting from merging the two input DataFrames.
    """
    common_keys = find_common_join_keys(enriched_activity_data, enriched_emp_hierarchy_data)
    if not common_keys:
        raise ValueError("No suitable join columns were identified between the two enriched DataFrames.")
    
    merged_data = enriched_activity_data.join(enriched_emp_hierarchy_data, common_keys, 'outer')
    return merged_data


def enhancement_workflow(spark, config):
    """
    Executes an ETL process based on the provided configuration, with a focus on flexibility and clarity.
    """
    try:
        primary_df = extract_incoming_vol_data(spark, config['primary_data_source'])

        enriched_activity_data = None
        if config.get('include_activity_data_enrichment', False):
            activity_list_df = extract_activity_list_data(spark, config['activity_list_data_source'])
            enriched_activity_data = enrich_primary_with_activity_data(primary_df, activity_list_df)

        enriched_emp_hierarchy_data = None
        if config.get('include_employee_hierarchy_enrichment', False):
            emp_hierarchy_df = extract_emp_hierarchy_data(spark, config['employee_hierarchy_data_source'])
            enriched_emp_hierarchy_data = enrich_primary_with_emp_hierarchy(
                primary_df, emp_hierarchy_df, config['employee_info_json_column'])

        # Merging step 2 and step 3 outputs if both are performed
        if enriched_activity_data is not None and enriched_emp_hierarchy_data is not None:
            final_output = merge_enriched_data(enriched_activity_data, enriched_emp_hierarchy_data)
        else:
            final_output = enriched_emp_hierarchy_data or enriched_activity_data

        if output_path := config.get('output_path'):
            final_output.write.mode("overwrite").parquet(output_path)
            print("Final output saved successfully.")
        else:
            return final_output

    except Exception as e:
        print(f"Failed to complete the enhancement_workflow due to: {e}")
        raise





