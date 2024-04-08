import unittest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession, Row
from etl_framework import FlexibleExtractor, DataJoinAndSave

class TestETLFramework(unittest.TestCase):
    def setUp(self):
        # Mock the SparkSession for all tests
        self.spark = Mock(spec=SparkSession)
        self.spark.read.parquet.return_value = Mock(name='DataFrame')
        self.spark.read.csv.return_value = Mock(name='DataFrame')
        self.spark.createDataFrame.return_value = Mock(name='DataFrame')

    @patch('etl_framework.SparkSessionManager.get_spark_session')
    def test_flexible_extractor_reads_parquet(self, get_spark_session_mock):
        get_spark_session_mock.return_value = self.spark
        extractor = FlexibleExtractor()

        _ = extractor.extract_data('/path/to/data.parquet')
        self.spark.read.parquet.assert_called_once_with('/path/to/data.parquet')

    @patch('etl_framework.SparkSessionManager.get_spark_session')
    def test_data_join_and_save_joins_dataframes_correctly(self, get_spark_session_mock):
        get_spark_session_mock.return_value = self.spark
        # Mock DataFrames for joining
        df1 = self.spark.createDataFrame([Row(id=1, name='Test1')])
        df2 = self.spark.createDataFrame([Row(id=1, attribute='Attribute1')])

        # Mock the extract_data method to return our predefined DataFrames
        with patch.object(FlexibleExtractor, 'extract_data', side_effect=[df1, df2]) as mock_method:
            data_join_save = DataJoinAndSave(self.spark)
            data_join_save.extract_and_join(
                extractor1=FlexibleExtractor(),
                extractor2=FlexibleExtractor(),
                source1='/path/to/source1',
                source2='/path/to/source2',
                join_columns=['id'],
                output_path='/path/to/output'
            )

            # Check if DataFrame joins are called correctly
            df1.join.assert_called_with(df2, ['id'])

if __name__ == '__main__':
    unittest.main()
