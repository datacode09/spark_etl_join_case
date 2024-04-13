from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, broadcast, sha2, approx_count_distinct, lit, coalesce


def decide_broadcast(df1: DataFrame, df2: DataFrame, join_key1: str, join_key2: str, spark: SparkSession) -> (bool, DataFrame):
    """
    Analyze two DataFrames and determine if one should be broadcasted based on the size and cardinality of the join keys.
    Returns a tuple with a boolean indicating if broadcasting is advised and the DataFrame recommended for broadcasting.
    """
    # Approximate distinct count of join keys
    df1_key_count = df1.select(approx_count_distinct(sha2(col(join_key1), 256))).first()[0]
    df2_key_count = df2.select(approx_count_distinct(sha2(col(join_key2), 256))).first()[0]

    # Get the broadcast threshold from Spark configuration
    threshold = int(spark.conf.get("spark.sql.autoBroadcastJoinThreshold", "10485760"))  # Default to 10MB

    # Estimate DataFrame sizes using count() and average row size approximation
    avg_row_size_df1 = sum(c.dataType.defaultSize() for c in df1.schema.fields)
    estimated_size_df1 = avg_row_size_df1 * df1.count()

    avg_row_size_df2 = sum(c.dataType.defaultSize() for c in df2.schema.fields)
    estimated_size_df2 = avg_row_size_df2 * df2.count()

    # Determine broadcasting based on the size and distinct key counts
    should_broadcast_df1 = estimated_size_df1 < threshold and df1_key_count < 5000
    should_broadcast_df2 = estimated_size_df2 < threshold and df2_key_count < 5000

    # Decide which DataFrame, if any, to broadcast
    if should_broadcast_df1 and should_broadcast_df2:
        # Choose the smaller DataFrame based on estimated size if both are below threshold
        return (True, df1 if estimated_size_df1 < estimated_size_df2 else df2)
    elif should_broadcast_df1:
        return (True, df1)
    elif should_broadcast_df2:
        return (True, df2)
    else:
        return (False, None)

def enrich_primary_with_activity_data(primary_df: DataFrame, activity_df: DataFrame, spark: SparkSession) -> DataFrame:
    logging.info("Enriching primary DataFrame with activity list data using Spark SQL.")

    # Register DataFrames as temporary views
    primary_df.createOrReplaceTempView("primary_view")
    activity_df.createOrReplaceTempView("activity_view")

    # Decide whether to broadcast and which DataFrame to broadcast
    should_broadcast, df_to_broadcast = decide_broadcast(primary_df, activity_df, "JoinKey", "JoinKey", spark)
    
    if should_broadcast:
        logging.info(f"Broadcasting DataFrame during join based on analysis.")
        if df_to_broadcast == primary_df:
            spark.catalog.cacheTable("primary_view")
        else:
            spark.catalog.cacheTable("activity_view")

    # Perform the join using SQL
    enriched_sql = """
    SELECT p.*, 
           COALESCE(a.New_Center, 'Not Defined') AS Center,
           COALESCE(a.Capacity_Planning_Group, 'Not Defined') AS Capacity_Planning_Group
    FROM primary_view p
    LEFT JOIN activity_view a ON p.JoinKey = a.JoinKey
    """
    enriched_output = spark.sql(enriched_sql)

    logging.info("Primary data enrichment with activity data completed using Spark SQL.")
    return enriched_output


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, broadcast, sha2, approx_count_distinct

def enrich_primary_with_emp_hierarchy(primary_df_with_join_key: DataFrame, emp_hierarchy_df: DataFrame, json_column_name: str, spark: SparkSession) -> DataFrame:
    logging.info("Enriching primary DataFrame with employee hierarchy data.")
    try:
        # Extract employee number from JSON column
        primary_df_with_employee_number = extract_employee_number_from_json(primary_df_with_join_key, json_column_name, "employee_number")
        
        # Determine if broadcasting is necessary and which DataFrame should be broadcast
        should_broadcast, df_to_broadcast = decide_broadcast(primary_df_with_employee_number, emp_hierarchy_df, "employee_number", "employeeid", spark)

        # Apply broadcasting decision
        if should_broadcast:
            if df_to_broadcast == primary_df_with_employee_number:
                logging.info("Broadcasting primary DataFrame based on analysis.")
                primary_df_with_employee_number = broadcast(primary_df_with_employee_number)
            else:
                logging.info("Broadcasting employee hierarchy DataFrame based on analysis.")
                emp_hierarchy_df = broadcast(emp_hierarchy_df)

        # Perform the join operation
        enriched_output = primary_df_with_employee_number.join(
            emp_hierarchy_df, 
            primary_df_with_employee_number["employee_number"] == emp_hierarchy_df["employeeid"], 
            "left_outer"
        )

        logging.info("Primary data enrichment with employee hierarchy completed.")
        return enriched_output
    except Exception as e:
        logging.error(f"Error enriching primary data with employee hierarchy: {str(e)}")
        raise

def merge_enriched_data(enriched_activity_data: DataFrame, enriched_emp_hierarchy_data: DataFrame, spark: SparkSession) -> DataFrame:
    logging.info("Merging enriched data from activity and employee hierarchy data.")

    # Determine if broadcasting is necessary and which DataFrame should be broadcast
    should_broadcast, df_to_broadcast = decide_broadcast(
        enriched_activity_data, enriched_emp_hierarchy_data, 
        "join_key_activity", "join_key_emp_hierarchy", spark
    )

    # Apply broadcasting decision
    if should_broadcast:
        if df_to_broadcast == enriched_activity_data:
            logging.info("Broadcasting activity data DataFrame for the merge operation.")
            enriched_activity_data = broadcast(enriched_activity_data)
        else:
            logging.info("Broadcasting employee hierarchy DataFrame for the merge operation.")
            enriched_emp_hierarchy_data = broadcast(enriched_emp_hierarchy_data)

    # Find common join keys to use for the merge
    common_keys = find_common_join_keys(enriched_activity_data, enriched_emp_hierarchy_data)
    if not common_keys:
        logging.warning("No suitable join columns identified; performing cross join.")
        merged_data = enriched_activity_data.crossJoin(enriched_emp_hierarchy_data)
    else:
        # Perform the join using the common keys found or perform an outer join
        merged_data = enriched_activity_data.join(
            enriched_emp_hierarchy_data, 
            common_keys, 
            'outer'
        )

    logging.info("Data merge completed.")
    return merged_data
