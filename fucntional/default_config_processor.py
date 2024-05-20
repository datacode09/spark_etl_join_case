import logging
import datetime
import json

def load_config(data_source):
    try:
        with open("configurations.json", "r") as file:
            all_configs = json.load(file)
            return all_configs.get(data_source, all_configs['default'])  # Use default if specific data source not found
    except FileNotFoundError:
        logging.error("Configuration file not found. Ensure 'configurations.json' exists.")
        raise
    except KeyError:
        logging.error("Data source configuration not found. Check your data source key.")
        raise

def enhancement_workflow(spark, primary_df, data_source="default"):
    config = load_config(data_source)

    logging.info("Starting the enhancement workflow with provided configuration")

    # The rest of your function implementation goes here...
    # ...
