import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AtomExtractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract_data(self, hdfs_path: str) -> DataFrame:
        try:
            df_hdfs = self.spark.read.parquet(hdfs_path)
            return df_hdfs
        except Exception as e:
            logging.error(f"Failed to extract data from HDFS at {hdfs_path}: {e}")
            raise

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

class Extraction:
    def __init__(self, spark: SparkSession, hdfs_path: str, nas_path: str):
        self.spark = spark
        self.hdfs_path = hdfs_path
        self.nas_path = nas_path

    def extract(self) -> (DataFrame, DataFrame):
        hdfs_extractor = AtomExtractor(self.spark)
        df_hdfs = hdfs_extractor.extract_data(self.hdfs_path)
        
        nas_extractor = ActivityListExtractor(self.spark)
        df_nas = nas_extractor.extract_data(self.nas_path)
        
        return df_hdfs, df_nas

class Transformation:
    @staticmethod
    def transform(df_hdfs: DataFrame, df_nas: DataFrame) -> DataFrame:
        df_transformed = df_hdfs.join(df_nas, df_hdfs.id == df_nas.id, "inner")
        return df_transformed

class Load:
    @staticmethod
    def load(df: DataFrame, target_path: str) -> None:
        df.write.mode("overwrite").parquet(target_path)

class AtomActivityMap:
    def __init__(self, hdfs_path: str, nas_path: str, target_path: str):
        self.spark = SparkSession.builder.appName("AtomActivityMapper").getOrCreate()
        self.hdfs_path = hdfs_path
        self.nas_path = nas_path
        self.target_path = target_path

    def run_etl(self):
        extraction = Extraction(self.spark, self.hdfs_path, self.nas_path)
        df_hdfs, df_nas = extraction.extract()
        
        df_transformed = Transformation.transform(df_hdfs, df_nas)
        
        Load.load(df_transformed, self.target_path)
        logging.info("ETL process completed successfully.")
        self.spark.stop()

# Example usage
if __name__ == "__main__":
    hdfs_path = "/path/to/hdfs"
    nas_path = "/path/to/nas/ATOM_Activity_List_00000.csv"
    target_path = "/path/to/target"
    etl_process = AtomActivityMap(hdfs_path, nas_path, target_path)
    etl_process.run_etl()
