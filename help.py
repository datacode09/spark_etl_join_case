from pyspark.sql import SparkSession
from etl_framework import IncomingVolExtractor, AtomExtractor, ActivityListExtractor, EmpHierarchyExtractor, DataJoinAndSave

def main():
    # Initialize Spark Session
    spark = SparkSession.builder.appName("ETLWorkflowGuide").getOrCreate()
    
    # Initialize extractors
    incoming_vol_extractor = IncomingVolExtractor(spark)
    atom_extractor = AtomExtractor(spark)
    activity_list_extractor = ActivityListExtractor(spark)
    emp_hierarchy_extractor = EmpHierarchyExtractor(spark)
    
    # Initialize DataJoinAndSave with the Spark session
    data_join_save = DataJoinAndSave(spark)
    
    # Define sources for each extractor
    incoming_vol_source = "path/to/incoming/volume/source"
    atom_source = "path/to/atom/source"
    activity_list_source = "path/to/activity/list"
    emp_hierarchy_source = "hive_table_for_emp_hierarchy"
    
    # Define join columns for each join operation
    join_columns = {
        'primary_activity': ['join_col'],
        'primary_emp_hierarchy': ['join_col'],
        'step_2_3': ['join_col']
    }
    
    # Scenario 1: Join Incoming Volume data with Activity List
    data_join_save.extract_join_and_optionally_save(
        primary_extractor=incoming_vol_extractor,
        source1=incoming_vol_source,
        extractor2=activity_list_extractor,
        source2=activity_list_source,
        join_columns=join_columns['primary_activity'],
        save_as_parquet=True,
        output_path="path/to/save/incoming_activity.parquet"
    )
    
    # Scenario 2: Similar process for Atom data joined with Activity List
    data_join_save.extract_join_and_optionally_save(
        primary_extractor=atom_extractor,
        source1=atom_source,
        extractor2=activity_list_extractor,
        source2=activity_list_source,
        join_columns=join_columns['primary_activity'],
        save_as_parquet=True,
        output_path="path/to/save/atom_activity.parquet"
    )
    
    # Additional Scenario: Join Incoming Volume or Atom data with Emp Hierarchy and Activity List, then combine
    # Step 1 & 2: Extract and join IncomingVol/Atom with ActivityList and EmpHierarchy separately
    incoming_activity_df = data_join_save.extract_join_and_optionally_save(
        incoming_vol_extractor, incoming_vol_source, activity_list_extractor, activity_list_source, join_columns['primary_activity'], save_as_parquet=False)
    
    incoming_emp_df = data_join_save.extract_join_and_optionally_save(
        incoming_vol_extractor, incoming_vol_source, emp_hierarchy_extractor, emp_hierarchy_source, join_columns['primary_emp_hierarchy'], save_as_parquet=False)
    
    # Step 3: Combine the results of Step 1 & 2
    final_df = data_join_save.join_dataframes(
        incoming_activity_df, incoming_emp_df, join_columns['step_2_3'], save_as_parquet=True, output_path="path/to/final_output.parquet")
    
    # Shutdown Spark session
    spark.stop()

if __name__ == "__main__":
    main()
