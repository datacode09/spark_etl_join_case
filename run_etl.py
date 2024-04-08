from pyspark.sql import SparkSession
from etl_framework import IncomingVolExtractor, AtomExtractor, ActivityListExtractor, EmpHierarchyExtractor, DataJoinAndSave

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETLEnhancementWorkflow").getOrCreate()
    
    # Initialize extractors
    primary_extractor = IncomingVolExtractor()  # or AtomExtractor()
    activity_list_extractor = ActivityListExtractor()
    emp_hierarchy_extractor = EmpHierarchyExtractor()

    # Initialize DataJoinAndSave
    data_join_save = DataJoinAndSave(spark)

    # Define sources
    primary_source = "path/to/incoming_vol/source"  # or "path/to/atom/source"
    activity_list_source = "path/to/activity_list/source"
    emp_hierarchy_source = "hive_table_for_emp_hierarchy"

    # Execute the workflow
    data_join_save.execute_enhancement_workflow(
        primary_extractor=primary_extractor,
        activity_list_extractor=activity_list_extractor,
        emp_hierarchy_extractor=emp_hierarchy_extractor,
        primary_source=primary_source,
        activity_list_source=activity_list_source,
        emp_hierarchy_source=emp_hierarchy_source,
        join_columns={
            'primary_activity': ['join_col1', 'join_col2'],
            'primary_emp_hierarchy': ['join_col1', 'join_col2'],
            'step_2_3': ['join_col1', 'join_col2']
        },
        save_as_parquet=True,
        intermediate_output_paths={
            'step_2': "path/to/save/step_2_output.parquet",
            'step_3': "path/to/save/step_3_output.parquet"
        },
        final_output_path="path/to/save/final_output.parquet"
    )
    
    spark.stop()
