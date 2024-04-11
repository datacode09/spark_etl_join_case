import json
from enhancement_module import get_spark_session, enhancement_workflow

def main():
    """ Main function to run the ETL processes with minimal setup. """
    try:
        # Configure logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # Load configuration
        with open('config.json', 'r') as config_file:
            config = json.load(config_file)

        # Run the ETL workflow
        etl_module.enhancement_workflow(spark=get_spark_session(app_name="enhancement_Framework"), config)
        logging.info("ETL workflow executed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during the ETL process: {e}")

if __name__ == "__main__":
    main()
