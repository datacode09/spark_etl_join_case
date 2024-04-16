import datetime
import logging

def enhancement_workflow(spark, primary_df, config=None):
    # Set default configuration if none provided
    if config is None:
        config = {
            "activity_list_data_source": "/path/to/activity/data",
            "activity_data_output_path": "/path/to/output/activity_data",
            "employee_hierarchy_output_path": "/path/to/output/employee_hierarchy",
            "output_path": "/path/to/final/output",
            "include_activity_data_enrichment": True,
            "include_employee_hierarchy_enrichment": True,
            "employee_info_json_column": "details_json"
        }
    
    logging.info("Starting the enhancement workflow with provided/default configuration")

    # Apply join key logic
    primary_df_with_join_key = incoming_vol_join_key_logic(primary_df)

    # Flags to determine if either enrichment is enabled
    activity_enrichment_enabled = config.get('include_activity_data_enrichment', False)
    employee_hierarchy_enrichment_enabled = config.get('include_employee_hierarchy_enrichment', False)

    # Current date for file naming
    current_date = datetime.datetime.now().strftime("%Y%m%d")

    enriched_activity_data = None
    enriched_emp_hierarchy_data = None

    try:
        # Enrichments processing
        if activity_enrichment_enabled:
            activity_schema = parse_schema(config['schemas']['activity_data_schema'])
            activity_list_df = extract_activity_list_data(spark, config['activity_list_data_source'], activity_schema)
            enriched_activity_data = enrich_primary_with_activity_data(primary_df_with_join_key, activity_list_df)
            # Save and log the activity data if path is provided
            activity_data_output_path = config['activity_data_output_path']
            activity_data_final_path = f"hdfs://{activity_data_output_path}/activity_data_{current_date}"
            enriched_activity_data.write.mode("overwrite").parquet(activity_data_final_path)
            logging.info(f"Enriched activity data saved at {activity_data_final_path}")

        if employee_hierarchy_enrichment_enabled:
            emp_hierarchy_df = extract_emp_hierarchy_data(spark)
            enriched_emp_hierarchy_data = enrich_primary_with_emp_hierarchy(primary_df_with_join_key, emp_hierarchy_df, config['employee_info_json_column'])
            # Save and log the employee hierarchy data if path is provided
            employee_hierarchy_output_path = config['employee_hierarchy_output_path']
            employee_hierarchy_final_path = f"hdfs://{employee_hierarchy_output_path}/employee_hierarchy_{current_date}"
            enriched_emp_hierarchy_data.write.mode("overwrite").parquet(employee_hierarchy_final_path)
            logging.info(f"Enriched employee hierarchy data saved at {employee_hierarchy_final_path}")

        # Final output handling and returning
        if activity_enrichment_enabled and employee_hierarchy_enrichment_enabled:
            final_output = merge_enriched_data(enriched_activity_data, enriched_emp_hierarchy_data)
            output_path = config['output_path']
            final_output_path = f"hdfs://{output_path}/output_{current_date}"
            final_output.write.mode("overwrite").parquet(final_output_path)
            logging.info(f"Final output saved at {final_output_path}")
            return final_output
        elif activity_enrichment_enabled:
            return enriched_activity_data
        elif employee_hierarchy_enrichment_enabled:
            return enriched_emp_hierarchy_data
        else:
            logging.warning("No enrichments were performed.")
            return primary_df_with_join_key

    except Exception as e:
        logging.error(f"Failed to complete the enhancement workflow due to: {e}")
        raise

