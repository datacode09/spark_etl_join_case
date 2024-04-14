import json
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, eval

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the Spark session
spark = SparkSession.builder.appName("ETLFrameworkTest").getOrCreate()

# Load configuration
with open('config.json', 'r') as f:
    config = json.load(f)

# Utility function to convert JSON schema to Spark schema
def get_schema(schema_list):
    return StructType([
        StructField(field['name'], eval(field['type']), field['nullable'])
        for field in schema_list
    ])

# Define the test functions
def test_get_spark_session_correct_app_name():
    app_name = "TestETLApp"
    session = SparkSession.builder.appName(app_name).getOrCreate()
    try:
        assert session.conf.get("spark.app.name") == app_name, "Application name should match the provided name"
        logging.info("Test passed: Correct application name initialized.")
    finally:
        session.stop()

def test_extract_incoming_vol_data_parquet_with_schema():
    test_path = config['primary_data_source']
    test_schema = get_schema(config['schemas']['primary_data_schema'])
    try:
        df = spark.read.schema(test_schema).parquet(test_path)
        assert df.schema == test_schema, "Schema should match the expected schema"
        logging.info("Test passed: Data extracted and schema validated successfully.")
    except Exception as e:
        logging.error(f"Test failed: {e}")

def test_find_common_join_keys():
    df1 = spark.createDataFrame([(1, 'a')], ["col1", "col2"])
    df2 = spark.createDataFrame([(1, 'b')], ["col3", "col2"])
    try:
        common_keys = set(df1.columns).intersection(df2.columns)
        assert 'col2' in common_keys, "Common keys should include 'col2'"
        logging.info("Test passed: Common join keys found successfully.")
    except Exception as e:
        logging.error(f"Test failed: {e}")

def test_incoming_vol_join_key_logic_and_transform():
    schema = StructType([
        StructField("data_source", StringType(), True),
        StructField("process", StringType(), True),
        StructField("subprocess_1", StringType(), True),
        StructField("subprocess_2", StringType(), True),
        StructField("subprocess_3", StringType(), True),
        StructField("subprocess_4", StringType(), True),
        StructField("subprocess_5", StringType(), True),
        StructField("subprocess_6", StringType(), True),
    ])

    data = [("cart", "Process1", "Sub1", "Sub2", "Sub3", "Sub4", "Sub5", "Sub6"),
            ("noncart", "process2", "sub1", "sub2", "sub3", "sub4", "sub5", "sub6")]

    df = spark.createDataFrame(data, schema)

    result_df = incoming_vol_join_key_logic(df)

    result_df.show(truncate=False)

    expected_join_keys = [
        "CART|PROCESS1|SUB1|SUB2|SUB3|SUB5|SUB6",
        "NONCART|PROCESS2|SUB1|SUB2|SUB3|SUB4|SUB5|SUB6|"
    ]

    actual_join_keys = [row['JoinKey'] for row in result_df.collect()]

    assert actual_join_keys == expected_join_keys, "JoinKey values are incorrect"

    logging.info("Test for transform and incoming_vol_join_key_logic passed successfully.")

    spark.stop()

def test_extract_activity_list_data_with_correct_schema():
    test_path = config['activity_list_data_source']
    test_schema = get_schema(config['schemas']['activity_data_schema'])
    try:
        df = spark.read.schema(test_schema).csv(test_path)
        assert df.schema == test_schema, "Schema should match the expected schema"
        logging.info("Test passed: Activity list data extracted with correct schema.")
    except Exception as e:
        logging.error(f"Test failed: {e}")

def test_extract_emp_hierarchy_data():
    # This test assumes Hive table access setup
    try:
        df = spark.sql("SELECT * FROM prod_rie0_atom.enterprisehierarchy")
        assert df.count() > 0, "Data should be retrieved from the Hive table"
        logging.info("Test passed: Employee hierarchy data extracted successfully.")
    except Exception as e:
        logging.error(f"Test failed: Hive table access issue: {e}")

def test_enrich_primary_with_activity_data():
    primary_df = spark.createDataFrame([(1, 'Key1'), (2, 'Key2')], ['id', 'JoinKey'])
    activity_list_df = spark.createDataFrame([('Key1', 'Activity1'), ('Key2', 'Activity2')], ['JoinKey', 'Activity'])
    try:
        enriched_df = primary_df.join(activity_list_df, 'JoinKey', 'left_outer')
        assert enriched_df.filter(enriched_df.Activity.isNotNull()).count() == 2, "All records should be enriched"
        logging.info("Test passed: Primary data enriched with activity data successfully.")
    except Exception as e:
        logging.error(f"Test failed: {e}")

def test_enrich_primary_with_emp_hierarchy():
    primary_df = spark.createDataFrame([(1, '12345')], ['id', 'employee_number'])
    emp_hierarchy_df = spark.createDataFrame([(12345, 'John Doe')], ['employeeid', 'employeename'])
    try:
        enriched_df = primary_df.join(emp_hierarchy_df, primary_df.employee_number == emp_hierarchy_df.employeeid, 'left_outer')
        assert enriched_df.filter(enriched_df.employeename.isNotNull()).count() == 1, "All records should be enriched"
        logging.info("Test passed: Primary data enriched with employee hierarchy successfully.")
    except Exception as e:
        logging.error(f"Test failed: {e}")

def test_extract_employee_number_from_json():
    data = [("{'employee_number': '12345'}",)]
    schema = StructType([StructField("details_json", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    try:
        df = df.withColumn("employee_number", col("details_json").getField("employee_number"))
        assert df.filter(df.employee_number == '12345').count() == 1, "Employee number should be extracted correctly"
        logging.info("Test passed: Employee number extracted correctly from JSON.")
    except Exception as e:
        logging.error(f"Test failed: {e}")


def main():
    # List all the test functions
    test_functions = [
        test_get_spark_session_correct_app_name,
        test_extract_incoming_vol_data_parquet_with_schema,
        test_find_common_join_keys,
        test_extract_activity_list_data_with_correct_schema,
        test_extract_emp_hierarchy_data,
        test_enrich_primary_with_activity_data,
        test_enrich_primary_with_emp_hierarchy,
        test_extract_employee_number_from_json,
        # Add additional tests as needed
    ]

    # Execute each test function
    logging.info("Starting all tests...")
    for test_func in test_functions:
        try:
            test_func()
            logging.info(f"{test_func.__name__}: Test executed successfully.")
        except Exception as e:
            logging.error(f"{test_func.__name__}: Test failed with error: {e}")

    # Shut down Spark session
    spark.stop()
    logging.info("All tests completed and Spark session closed.")

if __name__ == "__main__":
    main()


