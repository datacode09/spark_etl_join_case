from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import concat, col, lit
from pyspark.sql.types import StructType
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


class AtomExtractor(FlexibleExtractor):
    expected_schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
    ])

    def atom_join_key_logic(self, df):
        # Custom logic for AtomExtractor to create JoinKey
        return df.withColumn("JoinKey", concat(col("id"), lit('_'), col("name")))

class IncomingVolExtractor:
    expected_schema = StructType([
        StructField("data_source", StringType(), nullable=True),
        StructField("process", StringType(), nullable=True),
        StructField("subprocess_1", StringType(), nullable=True),
        StructField("subprocess_2", StringType(), nullable=True),
        StructField("subprocess_3", StringType(), nullable=True),
        StructField("subprocess_4", StringType(), nullable=True),
        StructField("subprocess_5", StringType(), nullable=True),
        StructField("subprocess_6", StringType(), nullable=True),
        # Add other fields as per your actual schema
    ])

    def __init__(self):
        self.spark = SparkSessionManager.get_spark_session()

    def extract_data(self, source) -> DataFrame:
        # Assuming source is a valid DataFrame or path
        df = self.spark.read.csv(source, header=True, inferSchema=True)
        return df

    def incoming_vol_join_key_logic(self, df):
        # Define columns to transform
        columns_to_transform = [
            "data_source", "process", "subprocess_1", "subprocess_2",
            "subprocess_3", "subprocess_4", "subprocess_5", "subprocess_6"
        ]
        
        # Define a function to apply the transformations
        def transform_column(col_name):
            return trim(upper(regexp_replace(col(col_name), "\\W", "")))

        # Apply transformations to specified columns
        transformed_columns = [transform_column(col_name).alias(col_name) for col_name in columns_to_transform]
        
        # Concatenate the transformed columns with the specified separators
        concatenated_col = concat_ws("|", 
                                     *transformed_columns[0:2], 
                                     lit("1"), 
                                     *transformed_columns[2:6], 
                                     lit(""), 
                                     transformed_columns[6], 
                                     lit("|"), 
                                     transformed_columns[7])
        
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
            rais
class EmpHierarchyExtractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract_data(self, hive_table: str) -> DataFrame:
        try:
            df_hive = self.spark.sql(f"SELECT * FROM {hive_table}")
            return df_hive
        except Exception as e:
            logging.error(f"Failed to extract data from Hive table {hive_table}: {e}")
            raise
