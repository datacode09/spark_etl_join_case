"""
test_get_spark_session: Tests if the get_spark_session function returns a SparkSession object.
test_extract_incoming_vol_data_parquet: Tests error handling when extracting data from a Parquet file without an expected schema.
test_extract_incoming_vol_data_csv_no_schema: Tests error handling when extracting data from a CSV file without an expected schema.
test_extract_incoming_vol_data_csv_with_schema: Tests if the schema of the extracted data matches the expected schema from a CSV file.
test_incoming_vol_join_key_logic: Tests the logic for generating join keys in the incoming_vol_join_key_logic function.
test_extract_activity_list_data: Tests if the schema of the extracted activity list data matches the expected schema.
test_extract_emp_hierarchy_data: Tests if the schema of the extracted employee hierarchy data matches the expected schema.
test_enrich_primary_with_activity_data: Tests if the schema of the enriched primary DataFrame with activity data matches the expected schema.
test_extract_employee_number_from_json: Tests if the schema of the DataFrame with extracted employee numbers from JSON matches the expected schema.
test_enrich_primary_with_emp_hierarchy: Tests if the schema of the enriched primary DataFrame with employee hierarchy matches the expected schema.
test_find_common_join_keys: Tests if the common join keys between two DataFrames are correctly identified.
test_merge_enriched_data_no_common_keys: Tests merging enriched data when no common keys are found.
test_merge_enriched_data_with_common_keys: Tests merging enriched data when common keys are found.
test_enhancement_workflow: Tests the entire enhancement workflow with a specified configuration.
These test cases cover various aspects of the module's functionality, including data extraction, transformation, enrichment, and error handling.
"""

import unittest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from module import (
    get_spark_session,
    extract_incoming_vol_data,
    incoming_vol_join_key_logic,
    extract_activity_list_data,
    extract_emp_hierarchy_data,
    enrich_primary_with_activity_data,
    extract_employee_number_from_json,
    enrich_primary_with_emp_hierarchy,
    find_common_join_keys,
    merge_enriched_data,
    enhancement_workflow
)

class TestModuleFunctions(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("Test").getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_get_spark_session(self):
        session = get_spark_session()
        self.assertIsInstance(session, SparkSession)

    def test_extract_incoming_vol_data_parquet(self):
        df = self.spark.createDataFrame([], "data_source STRING, event_id INT")
        with self.assertRaises(ValueError):
            extract_incoming_vol_data(self.spark, "path/to/parquet")

    def test_extract_incoming_vol_data_csv_no_schema(self):
        with self.assertRaises(ValueError):
            extract_incoming_vol_data(self.spark, "path/to/csv")

    def test_extract_incoming_vol_data_csv_with_schema(self):
        schema = self.spark.createDataFrame([], "data_source STRING, event_id INT").schema
        df = self.spark.createDataFrame([], "data_source STRING, event_id INT")
        result = extract_incoming_vol_data(self.spark, "path/to/csv", schema)
        self.assertEqual(result.schema, schema)

    def test_incoming_vol_join_key_logic(self):
        df = self.spark.createDataFrame([], "data_source STRING, process STRING, subprocess_1 STRING, subprocess_2 STRING, subprocess_3 STRING, subprocess_4 STRING, subprocess_5 STRING, subprocess_6 STRING")
        expected_df = self.spark.createDataFrame([], "data_source STRING, process STRING, subprocess_1 STRING, subprocess_2 STRING, subprocess_3 STRING, subprocess_4 STRING, subprocess_5 STRING, subprocess_6 STRING, JoinKey STRING")
        result = incoming_vol_join_key_logic(df)
        self.assertEqual(result.schema, expected_df.schema)

    def test_extract_activity_list_data(self):
        schema = self.spark.createDataFrame([], "DataSource STRING, SubProcess1 STRING, SubProcess2 STRING, SubProcess3 STRING, SubProcess4 STRING, SubProcess5 STRING, ActivityDescription STRING, Capacity STRING, SLAStart TIMESTAMP, SLAEnd TIMESTAMP, Identify STRING").schema
        df = self.spark.createDataFrame([], "DataSource STRING, SubProcess1 STRING, SubProcess2 STRING, SubProcess3 STRING, SubProcess4 STRING, SubProcess5 STRING, ActivityDescription STRING, Capacity STRING, SLAStart TIMESTAMP, SLAEnd TIMESTAMP, Identify STRING")
        result = extract_activity_list_data(self.spark, "path/to/csv", schema)
        self.assertEqual(result.schema, schema)

    def test_extract_emp_hierarchy_data(self):
        expected_df = self.spark.createDataFrame([], "employeeid STRING, employeename STRING, latest_snap_date TIMESTAMP")
        result = extract_emp_hierarchy_data(self.spark)
        self.assertEqual(result.schema, expected_df.schema)

    def test_enrich_primary_with_activity_data(self):
        primary_df = self.spark.createDataFrame([], "JoinKey STRING, data_source STRING")
        activity_list_df = self.spark.createDataFrame([], "JoinKey STRING, New_Center STRING, Capacity_Planning_Group STRING")
        expected_df = self.spark.createDataFrame([], "JoinKey STRING, data_source STRING, New_Center STRING, Capacity_Planning_Group STRING")
        result = enrich_primary_with_activity_data(primary_df, activity_list_df)
        self.assertEqual(result.schema, expected_df.schema)

    def test_extract_employee_number_from_json(self):
        df = self.spark.createDataFrame([("{'employee_number': '123'}",)], ["json_column"])
        expected_df = self.spark.createDataFrame([(123,)], ["employee_number"])
        result = extract_employee_number_from_json(df, "json_column", "employee_number")
        self.assertEqual(result.schema, expected_df.schema)

    def test_enrich_primary_with_emp_hierarchy(self):
        primary_df = self.spark.createDataFrame([], "JoinKey STRING, employee_number STRING")
        emp_hierarchy_df = self.spark.createDataFrame([], "employeeid STRING, employeename STRING")
        expected_df = self.spark.createDataFrame([], "JoinKey STRING, employee_number STRING, employeename STRING")
        result = enrich_primary_with_emp_hierarchy(primary_df, emp_hierarchy_df, "json_column")
        self.assertEqual(result.schema, expected_df.schema)

    def test_find_common_join_keys(self):
        df1 = self.spark.createDataFrame([], "JoinKey STRING, data_source STRING")
        df2 = self.spark.createDataFrame([], "JoinKey STRING, New_Center STRING")
        result = find_common_join_keys(df1, df2)
        self.assertEqual(result, ["JoinKey"])

    def test_merge_enriched_data_no_common_keys(self):
        df1 = self.spark.createDataFrame([], "JoinKey STRING, data_source STRING")
        df2 = self.spark.createDataFrame([], "New_Center STRING")
        result = merge_enriched_data(df1, df2)
        self.assertEqual(result.schema, df1.schema)

    def test_merge_enriched_data_with_common_keys(self):
        df1 = self.spark.createDataFrame([], "JoinKey STRING, data_source STRING")
        df2 = self.spark.createDataFrame([], "JoinKey STRING, New_Center STRING")
        result = merge_enriched_data(df1, df2)
        self.assertEqual(result.schema, df1.join(df2, "JoinKey").schema)

    def test_enhancement_workflow(self):
        config = {
            "primary_data_source": "path/to/parquet",
            "primary_data_schema": self.spark.createDataFrame([], "data_source STRING, event_id INT").schema,
            "include_activity_data_enrichment": True,
            "activity_list_data_source": "path/to/csv",
            "activity_data_schema": self.spark.createDataFrame([], "DataSource STRING, SubProcess1 STRING, SubProcess2 STRING, SubProcess3 STRING, SubProcess4 STRING, SubProcess5 STRING, ActivityDescription STRING, Capacity STRING, SLAStart TIMESTAMP, SLAEnd TIMESTAMP, Identify STRING").schema,
            "activity_data_output_path": "path/to/parquet",
            "include_employee_hierarchy_enrichment": True,
            "employee_info_json_column": "json_column",
            "employee_hierarchy_output_path": "path/to/parquet",
            "output_path": "path/to/parquet"
        }
        enhancement_workflow(self.spark, config)

if __name__ == "__main__":
    unittest.main()
