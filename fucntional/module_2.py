from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import coalesce, lit, concat, concat_ws, col, trim, upper, regexp_replace, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_spark_session(app_name="ETLFramework"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def extract_incoming_vol_data(spark, source, expected_schema: StructType) -> DataFrame:
    return spark.read.schema(expected_schema).csv(source, header=True, inferSchema=True)

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
