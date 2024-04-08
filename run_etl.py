from pyspark.sql import SparkSession
# Assuming the extractors and DataJoinAndSave classes are defined in etl_framework.py
from etl_framework import IncomingVolExtractor, ActivityListExtractor, EmpHierarchyExtractor, DataJoinAndSave

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("EnhancedETLWorkflow").getOrCreate()

    # Initialize extractor instances with Spark session
    primary_extractor = IncomingVolExtractor(spark)
    activity_list_extractor = ActivityListExtractor(spark)
    emp_hierarchy_extractor = EmpHierarchyExtractor(spark)

    # Initialize DataJoinAndSave instance with Spark session
    data_join_save = DataJoinAndSave(spark)

    # Define sources for extractors (replace with actual paths or sources)
    primary_source = "path/to/incoming/volume/data"
    activity_list_source = "path/to/activity/list/data"
    emp_hierarchy_source = "path/to/emp/hierarchy/data"

    # Define join columns for each join operation
    join_columns = {
        'primary_activity': 'JoinKey',
        'primary_emp_hierarchy': 'JoinKey',
        'step_2_3': 'JoinKey'
    }

    # Execute the ETL enhancement workflow
    data_join_save.execute_enhancement_workflow(
        primary_extractor=primary_extractor,
        activity_list_extractor=activity_list_extractor,
        emp_hierarchy_extractor=emp_hierarchy_extractor,
        primary_source=primary_source,
        activity_list_source=activity_list_source,
        emp_hierarchy_source=emp_hierarchy_source,
        join_columns=join_columns,
        save_as_parquet=True,  # Change to False if you wish to work with the DataFrame directly
        intermediate_output_paths={
            'step_2': "path/to/intermediate/step_2_output.parquet",
            'step_3': "path/to/intermediate/step_3_output.parquet"
        },
        final_output_path="path/to/final_output.parquet"
    )

    # Cleanup
    spark.stop()

if __name__ == "__main__":
    main()
