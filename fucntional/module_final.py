from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import coalesce, lit, concat, concat_ws, col, trim, upper, regexp_replace, when, get_json_object
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
import logging
import os
import pyarrow as pa

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_spark_session(app_name="ETLFramework"):
    logging.info("Initializing Spark session with app name: {}".format(app_name))
    return SparkSession.builder.appName(app_name).getOrCreate()


def extract_incoming_vol_data(spark: SparkSession, source, expected_schema: StructType = None) -> DataFrame:
    logging.info(f"Extracting data from source: {source}")
    try:
        # Initialize HDFS connection
        hdfs = pa.hdfs.connect()

        if isinstance(source, str):
            # Check if source is a directory or a file in HDFS
            if hdfs.isfile(source) and source.endswith('.parquet'):
                df = spark.read.parquet(source)
                logging.info("Data loaded from Parquet file.")
            elif hdfs.exists(source) and any(hdfs.isfile(os.path.join(source, f)) and f.endswith('.parquet') for f in hdfs.ls(source)):
                df = spark.read.parquet(source)
                logging.info("Data loaded from Parquet directory.")
            elif hdfs.exists(source):
                if not expected_schema:
                    error_msg = "Expected schema must be provided for CSV files."
                    logging.error(error_msg)
                    raise ValueError(error_msg)
                df = spark.read.schema(expected_schema).csv(source, header=True)
                logging.info("Data loaded from CSV file with explicit schema.")
            else:
                error_msg = "The specified HDFS path does not exist."
                logging.error(error_msg)
                raise FileNotFoundError(error_msg)

        elif isinstance(source, DataFrame):
            df = source
            logging.info("Data loaded directly from DataFrame input.")
        else:
            raise ValueError("Source must be a path (str) or a DataFrame.")

        # Perform schema validation if expected_schema is provided
        if expected_schema and df.schema != expected_schema:
            error_msg = "Data schema does not match the expected schema."
            logging.error(error_msg)
            raise ValueError(error_msg)

        # Close the HDFS connection
        hdfs.close()

        return df

    except Exception as e:
        logging.error(f"Failed to load data due to: {str(e)}")
        if 'hdfs' in locals():
            hdfs.close()
        raise






def incoming_vol_join_key_logic(df: DataFrame) -> DataFrame:
    logging.info("Applying incoming volume join key logic.")
    columns_to_transform = [
        "data_source", "process", "subprocess_1", "subprocess_2",
        "subprocess_3", "subprocess_5", "subprocess_6"
    ]

    df = df.withColumn(
        "JoinKey",
        when(col("data_source") == "CART",
             concat_ws("|", *[transform_column(c) for c in columns_to_transform], lit(""), transform_column("subprocess_6"), lit("|"))
        ).otherwise(
            concat_ws("|", *[transform_column(c) for c in columns_to_transform[:4]], transform_column("subprocess_4"), *[transform_column(c) for c in columns_to_transform[4:]], lit("|"))
        )
    )
    return df

def transform_column(col_name):
    return trim(upper(regexp_replace(col(col_name), "\\W", "")))

def extract_activity_list_data(spark, nas_path: str, expected_schema: StructType) -> DataFrame:
    logging.info("Extracting activity list data from: {}".format(nas_path))
    if not expected_schema:
        error_msg = "Expected schema must be provided for activity list data."
        logging.error(error_msg)
        raise ValueError(error_msg)
    try:
        df = spark.read.schema(expected_schema).csv(nas_path, header=True)
        
        # Schema verification
        if not df.schema == expected_schema:
            error_msg = "Schema mismatch between expected and actual data."
            logging.error(error_msg)
            raise ValueError(error_msg)
        
        return df
    except Exception as e:
        logging.error("Failed to extract data from NAS at {}: {}".format(nas_path, e))
        raise

def extract_emp_hierarchy_data(spark) -> DataFrame:
    logging.info("Extracting employee hierarchy data.")
    hive_table = "prod_rie0_atom.enterprisehierarchy"
    try:
        return spark.sql(f"SELECT employeeid, employeename, MAX(snap_date) AS latest_snap_date FROM {hive_table} GROUP BY employeeid, employeename")
    except Exception as e:
        logging.error("Failed to extract data from Hive table {}: {}".format(hive_table, e))
        raise

def enrich_primary_with_activity_data(primary_df_with_join_key: DataFrame, activity_list_df: DataFrame) -> DataFrame:
    logging.info("Enriching primary DataFrame with activity list data.")
    try:
        enriched_output = primary_df_with_join_key.join(
            activity_list_df, 
            primary_df_with_join_key["JoinKey"] == activity_list_df["JoinKey"], 
            "left_outer"
        ).select(
            primary_df_with_join_key["*"],
            coalesce(activity_list_df["New_Center"], lit("Not Defined")).alias("Center"),
            coalesce(activity_list_df["Capacity_Planning_Group"], lit("Not Defined")).alias("Capacity_Planning_Group")
        )
        logging.info("Primary data enrichment with activity data completed.")
        return enriched_output
    except Exception as e:
        logging.error("Error enriching primary data with activity data: {}".format(e))
        raise

def extract_employee_number_from_json(df: DataFrame, json_column_name: str, key_name: str) -> DataFrame:
    logging.info("Extracting employee number from JSON column.")
    json_path = f"$.{key_name}"
    try:
        return df.withColumn("employee_number", get_json_object(col(json_column_name), json_path))
    except Exception as e:
        logging.error("Failed to extract employee number from JSON: {}".format(e))
        raise

def enrich_primary_with_emp_hierarchy(primary_df_with_join_key: DataFrame, emp_hierarchy_df: DataFrame, json_column_name: str) -> DataFrame:
    logging.info("Enriching primary DataFrame with employee hierarchy data.")
    try:
        primary_df_with_employee_number = extract_employee_number_from_json(primary_df_with_join_key, json_column_name, "employee_number")
        enriched_output = primary_df_with_employee_number.join(
            emp_hierarchy_df, 
            primary_df_with_employee_number["employee_number"] == emp_hierarchy_df["employeeid"], 
            "left_outer"
        )
        logging.info("Primary data enrichment with employee hierarchy completed.")
        return enriched_output
    except Exception as e:
        logging.error("Error enriching primary data with employee hierarchy: {}".format(e))
        raise

def find_common_join_keys(df1: DataFrame, df2: DataFrame) -> list:
    logging.info("Finding common join keys between two DataFrames.")
    df1_columns = set(df1.columns)
    df2_columns = set(df2.columns)
    common_columns = list(df1_columns.intersection(df2_columns))
    if not common_columns:
        logging.warning("No common join keys found.")
    return common_columns

def merge_enriched_data(enriched_activity_data: DataFrame, enriched_emp_hierarchy_data: DataFrame) -> DataFrame:
    logging.info("Merging enriched data from activity and employee hierarchy data.")
    common_keys = find_common_join_keys(enriched_activity_data, enriched_emp_hierarchy_data)
    if not common_keys:
        logging.warning("No suitable join columns identified; performing cross join.")
        return enriched_activity_data.crossJoin(enriched_emp_hierarchy_data)

    merged_data = enriched_activity_data.join(enriched_emp_hierarchy_data, common_keys, 'outer')
    logging.info("Data merge completed.")
    return merged_data

# Define a function to parse JSON schema into PySpark StructType
def parse_schema(schema_json):
    from pyspark.sql.types import StringType, IntegerType, TimestampType, BooleanType, StructField
    # Map string type names to PySpark classes
    type_mapping = {
        "StringType": StringType,
        "IntegerType": IntegerType,
        "TimestampType": TimestampType,
        "BooleanType": BooleanType
    }
    # Convert list of field definitions into StructType
    return StructType([
        StructField(field['name'], type_mapping[field['type']](), nullable=field['nullable'])
        for field in schema_json
    ])

# Main workflow function
def enhancement_workflow(spark, config):
    # Log the beginning of the process
    logging.info("Starting the enhancement workflow with configuration: {}".format(config))
    try:
        # Parse primary data schema from configuration and extract data
        primary_schema = parse_schema(config.get('schemas')['primary_data_schema'])
        primary_df = extract_incoming_vol_data(spark, config.get('primary_data_source'), primary_schema)

        # Check if activity data enrichment is enabled in the configuration
        if config.get('include_activity_data_enrichment', False):
            # Parse activity data schema and extract data
            activity_schema = parse_schema(config.get('schemas')['activity_data_schema'])
            activity_list_df = extract_activity_list_data(spark, config.get('activity_list_data_source'), activity_schema)
            # Enrich primary data with activity data
            enriched_activity_data = enrich_primary_with_activity_data(primary_df, activity_list_df)
            
            # Check if there is a configured path for outputting enriched activity data
            activity_data_output_path = config.get('activity_data_output_path')
            if activity_data_output_path:
                # Write enriched data to specified path
                enriched_activity_data.write.mode("overwrite").parquet(activity_data_output_path)
                logging.info("Enriched activity data saved at {}".format(activity_data_output_path))

        # Check if employee hierarchy enrichment is enabled in the configuration
        if config.get('include_employee_hierarchy_enrichment', False):
            # Extract employee hierarchy data (assuming function is defined elsewhere)
            emp_hierarchy_df = extract_emp_hierarchy_data(spark)
            # Enrich primary data with employee hierarchy data using a specific column for joining
            enriched_emp_hierarchy_data = enrich_primary_with_emp_hierarchy(primary_df, emp_hierarchy_df, config.get('employee_info_json_column'))
            
            # Check if there is a configured path for outputting enriched employee hierarchy data
            employee_hierarchy_output_path = config.get('employee_hierarchy_output_path')
            if employee_hierarchy_output_path:
                # Write enriched data to specified path
                enriched_emp_hierarchy_data.write.mode("overwrite").parquet(employee_hierarchy_output_path)
                logging.info("Enriched employee hierarchy data saved at {}".format(employee_hierarchy_output_path))

        # Check if there is a final output path configured for merged data
        output_path = config.get('output_path')
        if output_path:
            logging.info("Final merged output path specified but merge logic not implemented.")
            # Merge enriched data sets (assuming function is defined elsewhere)
            final_output = merge_enriched_data(enriched_activity_data, enriched_emp_hierarchy_data)
            # Write final output to specified path
            final_output.write.mode("overwrite").parquet(output_path)
            logging.info("Final output saved at {}".format(output_path))
    except Exception as e:
        # Log any exceptions that occur during the process
        logging.error("Failed to complete the enhancement_workflow due to: {}".format(e))
        raise

