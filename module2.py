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

class AtomExtractor(BaseExtractor):
    expected_schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        # Add more fields as per your actual schema
    ])

    def extract_data(self, source) -> DataFrame:
        df = super().extract_data(source)
        self.validate_schema(df, self.expected_schema)
        df_transformed = self.add_join_key(df, self.atom_join_key_logic)
        return df_transformed

    def atom_join_key_logic(self, df):
        # Custom logic for AtomExtractor to create JoinKey
        return df.withColumn("JoinKey", concat(col("id"), lit('_'), col("name")))

class IncomingVolExtractor(BaseExtractor):
    expected_schema = StructType([
        StructField("volume", IntegerType(), nullable=False),
        StructField("date", StringType(), nullable=False),
        # Define your actual schema
    ])

    def extract_data(self, source) -> DataFrame:
        df = super().extract_data(source)
        self.validate_schema(df, self.expected_schema)
        df_transformed = self.add_join_key(df, self.incoming_vol_join_key_logic)
        return df_transformed
