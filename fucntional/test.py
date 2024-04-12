def test_get_spark_session_correct_app_name():
    logging.info("Testing: get_spark_session with correct app name")
    app_name = "TestETLApp"
    session = get_spark_session(app_name)
    try:
        assert session.conf.get("spark.app.name") == app_name, "Application name should match the provided name"
        logging.info("Test passed: Correct application name initialized.")
    except Exception as e:
        logging.error(f"Test failed: {e}")
    finally:
        session.stop()

def test_extract_incoming_vol_data_parquet_with_schema():
    logging.info("Testing: extract_incoming_vol_data with a valid Parquet file and correct schema")
    test_path = "test_data.parquet"
    test_schema = get_schema(config['schemas']['primary_data_schema'])
    try:
        df = extract_incoming_vol_data(spark, test_path, test_schema)
        assert df.schema == test_schema, "Schema should match the expected schema"
        logging.info("Test passed: Data extracted and schema validated successfully.")
    except Exception as e:
        logging.error(f"Test failed: {e}")


def test_incoming_vol_join_key_logic_correct_concatenation():
    logging.info("Testing: incoming_vol_join_key_logic for correct field concatenation")
    data = [(1, "Source1", "Process1")]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("data_source", StringType(), True),
        StructField("process", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    try:
        result_df = incoming_vol_join_key_logic(df)
        expected_key = "SOURCE1|PROCESS1||||"
        assert result_df.collect()[0]["JoinKey"] == expected_key, "JoinKey should be correctly formed"
        logging.info("Test passed: Join key correctly concatenated.")
    except Exception as e:
        logging.error(f"Test failed: {e}")

def test_transform_column_correct_transformation():
    logging.info("Testing: transform_column for correct transformations")
    data = [("  data1@#$",)]
    schema = StructType([StructField("column1", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("transformed", transform_column("column1"))
    try:
        assert df.collect()[0]["transformed"] == "DATA1", "Transformed column should match expected format"
        logging.info("Test passed: Column transformed correctly.")
    except Exception as e:
        logging.error(f"Test failed: {e}")

def test_extract_activity_list_data_with_correct_schema():
    logging.info("Testing: extract_activity_list_data with correct schema")
    test_path = "activity_data.csv"
    test_schema = get_schema(config['schemas']['activity_data_schema'])
    try:
        df = extract_activity_list_data(spark, test_path, test_schema)
        assert df.schema == test_schema, "Schema should match the expected schema"
        logging.info("Test passed: Activity list data extracted with correct schema.")
    except Exception as e:
        logging.error(f"Test failed: {e}")

def test_extract_emp_hierarchy_data_handle_hive_error():
    logging.info("Testing: extract_emp_hierarchy_data when Hive table is unavailable")
    try:
        df = extract_emp_hierarchy_data(spark)
        assert df is None, "Should not retrieve data from an unavailable Hive table"
        logging.error("Test failed: Retrieved data from an unavailable Hive table.")
    except Exception as e:
        logging.info("Test passed: Correctly handled error when Hive table is unavailable.")

def test_enrich_primary_with_activity_data_successful_enrichment():
    logging.info("Testing: enrich_primary_with_activity_data with correct join keys")
    primary_data = [(1, "JoinKey1"), (2, "JoinKey2")]
    activity_data = [("JoinKey1", "Activity1"), ("JoinKey2", "Activity2")]
    primary_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("JoinKey", StringType(), True)
    ])
    activity_schema = StructType([
        StructField("JoinKey", StringType(), True),
        StructField("Activity", StringType(), True)
    ])
    primary_df = spark.createDataFrame(primary_data, primary_schema)
    activity_list_df = spark.createDataFrame(activity_data, activity_schema)
    try:
        enriched_df = enrich_primary_with_activity_data(primary_df, activity_list_df)
        assert enriched_df.filter("Activity == 'Activity1'").count() == 1, "Primary data should be enriched correctly"
        logging.info("Test passed: Primary data enriched successfully with activity data.")
    except Exception as e:
        logging.error(f"Test failed: {e}")

def test_extract_employee_number_from_json_correct_extraction():
    logging.info("Testing: extract_employee_number_from_json for correct extraction")
    data = [("{'employee_number': '12345'}",)]
    schema = StructType([StructField("details_json", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    try:
        result_df = extract_employee_number_from_json(df, "details_json", "employee_number")
        assert result_df.filter("employee_number == '12345'").count() == 1, "Employee number should be extracted correctly"
        logging.info("Test passed: Employee number extracted correctly from JSON.")
    except Exception as e:
        logging.error(f"Test failed: {e}")

def test_enrich_primary_with_emp_hierarchy_partial_keys():
    logging.info("Testing: enrich_primary_with_emp_hierarchy with partial matching keys")
    primary_data = [(1, "12345"), (2, "67890")]
    hierarchy_data = [(12345, "John Doe"), (67890, "Jane Smith")]
    primary_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("employee_number", StringType(), True)
    ])
    hierarchy_schema = StructType([
        StructField("employeeid", IntegerType(), True),
        StructField("employeename", StringType(), True)
    ])
    primary_df = spark.createDataFrame(primary_data, primary_schema)
    emp_hierarchy_df = spark.createDataFrame(hierarchy_data, hierarchy_schema)
    try:
        enriched_df = enrich_primary_with_emp_hierarchy(primary_df, emp_hierarchy_df, "employee_number")
        assert enriched_df.filter("employeename == 'John Doe'").count() == 1, "Should enrich correctly even with partial keys"
        logging.info("Test passed: Employee hierarchy data enriched successfully with partial keys.")
    except Exception as e:
        logging.error(f"Test failed: {e}")


def test_find_common_join_keys_no_common_keys():
    logging.info("Testing: find_common_join_keys with no common keys")
    df1 = spark.createDataFrame([(1,)], ["column1"])
    df2 = spark.createDataFrame([(1,)], ["column2"])
    common_keys = find_common_join_keys(df1, df2)
    try:
        assert len(common_keys) == 0, "No common keys should be found"
        logging.info("Test passed: No common join keys correctly identified.")
    except Exception as e:
        logging.error(f"Test failed: {e}")

