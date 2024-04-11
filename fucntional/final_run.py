import json
from enhancement_module import get_spark_session, enhancement_workflow

def main():
    """ Main function to run the ETL processes with minimal setup. """
    try:
        # Get the current date and time
        current_time = datetime.now()
        
        # Format the current date and time as a string suitable for filenames
        formatted_time = current_time.strftime('%Y-%m-%d_%H-%M-%S')
        
        # Create a dynamic log file name
        log_filename = f'app_{formatted_time}.log'
        
        # Configure logging to write to a dynamically named file
        logging.basicConfig(filename=log_filename, filemode='w', level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')

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
