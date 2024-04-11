import json
import logging
from pyspark.sql import SparkSession

# Import the module containing the ETL functions
import etl_module

def main():
    """ Main function to run the ETL processes with minimal setup. """
    try:
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # Load configuration
        with open('config.json', 'r') as config_file:
            config = json.load(config_file)

        # Start the Spark session
        spark = SparkSession.builder.appName("EnhancedETLWorkflow").getOrCreate()
        logging.info("Spark session initialized.")

        # Run the ETL workflow
        etl_module.enhancement_workflow(spark, config)
        logging.info("ETL workflow executed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during the ETL process: {e}")

if __name__ == "__main__":
    main()
