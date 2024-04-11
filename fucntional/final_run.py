from etl_framework import enhancement_workflow, get_spark_session

def main():
    # Initialize the Spark session
    spark = get_spark_session("EnhancedETLApp")

    # Define the configuration for the ETL workflow
    config = {
        'primary_data_source': '/data/primary_dataset.parquet',
        'activity_list_data_source': '/data/activity_list.csv',
        'employee_info_json_column': 'additional_features',
        'include_activity_data_enrichment': True,
        'activity_data_output_path': '/output/enriched_activity_data.parquet',  # Path to save enriched activity data
        'include_employee_hierarchy_enrichment': True,
        'employee_hierarchy_output_path': '/output/enriched_emp_hierarchy_data.parquet',  # Path to save enriched employee hierarchy data
        'output_path': '/output/final_merged_data.parquet'  # Path for saving the final merged output, if needed
    }

    # Execute the ETL workflow with the provided configuration
    enhancement_workflow(spark, config)

    # Stop the Spark session after the ETL process completes
    spark.stop()

if __name__ == "__main__":
    main()
