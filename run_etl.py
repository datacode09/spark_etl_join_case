# run_etl_process.py
from etl_framework import SparkSessionManager, AtomExtractor, IncomingVolExtractor, ActivityListExtractor, EmpHierarchyExtractor, DataJoinAndSave

if __name__ == "__main__":
    spark = SparkSessionManager.get_spark_session()
    
    # Initialize extractors with any required parameters
    atom_extractor = AtomExtractor()
    incoming_vol_extractor = IncomingVolExtractor()
    activity_list_extractor = ActivityListExtractor(filename="example.csv")
    emp_hierarchy_extractor = EmpHierarchyExtractor()
    
    # Initialize the DataJoinAndSave instance
    data_join_save = DataJoinAndSave(spark)
    
    # Example: Join AtomExtractor output with ActivityListExtractor and save
    data_join_save.extract_and_join(atom_extractor, activity_list_extractor,
                                    "path/to/atom/source", "path/to/activity/list",
                                    ["join_column"], "path/to/output/atom_activity_combined.parquet")
    
    # Example: Load the combined output, join with EmpHierarchyExtractor output, and save
    data_join_save.join_with_emp_hierarchy("path/to/output/atom_activity_combined.parquet", "hive_table_for_emp_hierarchy",
                                           ["join_column"], "path/to/final/output/atom_emp_combined.parquet")
    
    SparkSessionManager.stop_spark_session()
