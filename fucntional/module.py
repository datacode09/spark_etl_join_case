from pyspark.sql import SparkSession
# Assuming the extractors and DataJoinAndSave functions are adapted based on the previous conversion guidance
from etl_framework_functional import (
    extract_incoming_vol_data,
    extract_activity_list_data,
    extract_emp_hierarchy_data,
    join_and_save_data
)

def setup_spark_session():
    return SparkSession.builder.appName("EnhancedETLWorkflow").getOrCreate()

def execute_etl_workflow(spark, primary_source, activity_list_source, emp_hierarchy_source, join_columns, intermediate_output_paths, final_output_path):
    primary_df = extract_incoming_vol_data(spark, primary_source)
    activity_list_df = extract_activity_list_data(spark, activity_list_source)
    emp_hierarchy_df = extract_emp_hierarchy_data(spark, emp_hierarchy_source)

    join_and_save_data(
        spark,
        primary_df,
        activity_list_df,
        emp_hierarchy_df,
        join_columns,
        save_as_parquet=True,
        intermediate_output_paths=intermediate_output_paths,
        final_output_path=final_output_path
    )

def main():
    spark = setup_spark_session()

    primary_source = "path/to/incoming/volume/data"
    activity_list_source = "path/to/activity/list/data"
    emp_hierarchy_source = "path/to/emp/hierarchy/data"

    join_columns = {
        'primary_activity': 'JoinKey',
        'primary_emp_hierarchy': 'JoinKey',
        'step_2_3': 'JoinKey'
    }

    execute_etl_workflow(
        spark,
        primary_source,
        activity_list_source,
        emp_hierarchy_source,
        join_columns,
        intermediate_output_paths={
            'step_2': "path/to/intermediate/step_2_output.parquet",
            'step_3': "path/to/intermediate/step_3_output.parquet"
        },
        final_output_path="path/to/final_output.parquet"
    )

    spark.stop()

if __name__ == "__main__":
    main()
