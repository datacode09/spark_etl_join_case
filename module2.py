from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, trim, upper, concat_ws, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
from pyspark.sql.functions import coalesce, lit
import os
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SparkSessionManager:
    _session = None

    @classmethod
    def get_spark_session(cls):
        if cls._session is None:
            cls._session = SparkSession.builder.appName("ETLFramework").getOrCreate()
        return cls._session

    @classmethod
    def stop_spark_session(cls):
        if cls._session is not None:
            cls._session.stop()
            cls._session = None

class BaseExtractor:
    def __init__(self):
        self.spark = SparkSessionManager.get_spark_session()

    def validate_schema(self, df: DataFrame, expected_schema: StructType):
        if not df.schema.simpleString() == expected_schema.simpleString():
            raise ValueError("DataFrame schema does not match expected schema.")

    def add_join_key(self, df: DataFrame, formula) -> DataFrame:
        """
        Applies a custom transformation formula to add a 'JoinKey' column to the DataFrame.

        Example formula usage within a subclass:
        
        def custom_join_key_formula(df):
            # Example transformation logic to concatenate 'id' and 'name' columns
            return df.withColumn("JoinKey", concat(col("id"), lit('_'), col("name")))
        
        Then call it as:
        df_transformed = self.add_join_key(df, self.custom_join_key_formula)

        ======================================================
        class CustomExtractor(BaseExtractor):
            def __init__(self):
                super().__init__()
        
            def extract_data(self, source: str) -> DataFrame:
                # Data extraction logic (e.g., read from a source)
                df = self.spark.read.csv(source, header=True)
                return df
        
            def transform_data(self, df: DataFrame) -> DataFrame:
                # Define a custom transformation formula as a method
                def custom_join_key_formula(df):
                    # Assume 'id' and 'name' are columns in the DataFrame
                    return df.withColumn("JoinKey", concat(col("id"), lit('_'), col("name")))
                
                # Apply the custom formula to add 'JoinKey'
                df_transformed = self.add_join_key(df, custom_join_key_formula)
                return df_transformed

        """
        return formula(df)

class FlexibleExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()

    def extract_data(self, source) -> DataFrame:
        if isinstance(source, str):
            if source.endswith('.parquet'):
                df = self.spark.read.parquet(source)
            elif os.path.isdir(source):
                df = self.spark.read.parquet(f"{source}/*.parquet")
            else:
                raise ValueError("Invalid path provided. Path must be to a .parquet file or a directory.")
        elif isinstance(source, DataFrame):
            df = source
        else:
            raise ValueError("Source must be a path (str) or a DataFrame.")
        return df


# class AtomExtractor(FlexibleExtractor):
#     expected_schema = StructType([
#         StructField("id", IntegerType(), nullable=False),
#         StructField("name", StringType(), nullable=True),
#     ])

#     def atom_join_key_logic(self, df):
#         # Custom logic for AtomExtractor to create JoinKey
#         return df.withColumn("JoinKey", concat(col("id"), lit('_'), col("name")))

class IncomingVolExtractor:
    expected_schema = StructType([
        StructField("event_id", IntegerType(), True),
        StructField("data_source", StringType(), False),
        StructField("activity_reference_id", StringType(), True),
        StructField("activity_received_timestamp", TimestampType(), True),
        StructField("activity_latest_timestamp", TimestampType(), True),
        StructField("activity_latest_status", StringType(), True),
        StructField("touch_start_timestamp", TimestampType(), True),
        StructField("touch_status", StringType(), True),
        StructField("process", StringType(), False),
        StructField("subprocess_1", StringType(), True),
        StructField("subprocess_2", StringType(), True),
        StructField("subprocess_3", StringType(), True),
        StructField("subprocess_4", StringType(), True),
        StructField("subprocess_5", StringType(), True),
        StructField("subprocess_6", StringType(), True),
        StructField("incoming_volume", IntegerType(), True),
        StructField("duration", IntegerType(), True),
        StructField("csc_date", TimestampType(), True),
        StructField("within_sla", BooleanType(), True),
        StructField("additional_features", StringType(), True),
    ])

    def __init__(self):
        self.spark = SparkSessionManager.get_spark_session()

    def extract_data(self, source) -> DataFrame:
        # Assuming source is a valid DataFrame or path
        df = self.spark.read.schema(self.expected_schema).csv(source, header=True, inferSchema=True)
        return df

    def incoming_vol_join_key_logic(self, df):
        # Define a function to apply the transformations
        def transform_column(col_name):
            return trim(upper(regexp_replace(col(col_name), "\\W", "")))

        # Apply transformations to specified columns, excluding subprocess_4 initially
        transformed_columns = [transform_column(c) for c in [
            "data_source", "process", "subprocess_1", "subprocess_2",
            "subprocess_3", "subprocess_5", "subprocess_6"]]

        # Concatenate the transformed columns with the specified separators,
        # conditionally including subprocess_4 based on data_source
        concatenated_col = when(col("data_source") == "CART",
                                concat_ws("|", *transformed_columns, lit("|")))
        .otherwise(concat_ws("|", *transformed_columns[:4], transform_column("subprocess_4"), *transformed_columns[4:], lit("|")))

        # Add the concatenated column to the DataFrame
        df_with_join_key = df.withColumn("JoinKey", concatenated_col)
        
        return df_with_join_key




class ActivityListExtractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract_data(self, nas_path: str) -> DataFrame:
        try:
            df_nas = self.spark.read.csv(nas_path, header=True, inferSchema=True)
            return df_nas
        except Exception as e:
            logging.error(f"Failed to extract data from NAS at {nas_path}: {e}")
            raise
class EmpHierarchyExtractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract_data(self) -> DataFrame:
        """
        Extracts the latest snapshot of employee hierarchy data from a predefined Hive table.
        
        :return: A DataFrame containing the query results.
        """
        try:
            # Embedded Hive table name
            hive_table = "prod_rie0_atom.enterprisehierarchy"
            sql_query = f"""
            SELECT employeeid, employeename, MAX(snap_date) AS latest_snap_date
            FROM {hive_table}
            GROUP BY employeeid, employeename
            """
            df_hive = self.spark.sql(sql_query)
            return df_hive
        except Exception as e:
            logging.error(f"Failed to extract data from Hive table {hive_table}: {e}")
            raise

class DataJoinAndSave:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def execute_enhancement_workflow(self, primary_extractor, activity_list_extractor, emp_hierarchy_extractor, primary_source, activity_list_source, emp_hierarchy_source, join_columns, save_as_parquet=False, intermediate_output_paths=None, final_output_path=None):
        """
        Executes the enhancement workflow with conditional logic for Center and Capacity_Planning_Group columns.
        """
        # Extract data using the primary extractor
        primary_df = primary_extractor.extract_data(primary_source)

        # Join with ActivityListExtractor output, adding Center and Capacity_Planning_Group columns
        activity_df = activity_list_extractor.extract_data(activity_list_source)
        step_2_output = primary_df.join(activity_df, join_columns['primary_activity'], "left") \
            .select(
                primary_df["*"], 
                coalesce(activity_df["New_Center"], lit("Not Defined")).alias("Center"),
                coalesce(activity_df["Capacity_Planning_Group"], lit("Not Defined")).alias("Capacity_Planning_Group")
            )

        # Join with EmpHierarchyExtractor output
        emp_hierarchy_df = emp_hierarchy_extractor.extract_data(emp_hierarchy_source)
        step_3_output = primary_df.join(emp_hierarchy_df, join_columns['primary_emp_hierarchy'])

        # Join step 2 and step 3 outputs, this time without adding new columns
        final_output = step_2_output.join(step_3_output, join_columns['step_2_3'], 'outer')

        # Optionally save outputs
        if save_as_parquet:
            if intermediate_output_paths:
                step_2_output.write.mode("overwrite").parquet(intermediate_output_paths.get('step_2', 'step_2_output.parquet'))
                step_3_output.write.mode("overwrite").parquet(intermediate_output_paths.get('step_3', 'step_3_output.parquet'))
            if final_output_path:
                final_output.write.mode("overwrite").parquet(final_output_path)
        else:
            return final_output
