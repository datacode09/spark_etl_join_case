from pyspark.sql import SparkSession
from timeseries_utils import calculate_snap_dates, create_snap_date_column

# Create a Spark session
spark = SparkSession.builder.appName("TimeSeriesTest").getOrCreate()

# Load the sample CSV data into a Spark DataFrame
sample_csv_path = '/mnt/data/sample_employee_data.csv'

# Define the schema for the CSV
schema = "EmployeeID STRING, DataSource STRING, StartDate DATE, EndDate DATE, Comments STRING, Column1 STRING"

# Load the data
df = spark.read.csv(sample_csv_path, schema=schema, header=True)

# Calculate the min and max dates
min_date, max_date = calculate_snap_dates(df)
print(f"Min Start Date: {min_date}")
print(f"Max End Date: {max_date}")

# Create the snap_date column
df_with_snap_date = create_snap_date_column(df)

# Show the resulting DataFrame
df_with_snap_date.show()

# Optionally, save the resulting DataFrame to a new CSV
output_csv_path = '/mnt/data/employee_data_with_snap_dates.csv'
df_with_snap_date.write.csv(output_csv_path, header=True)
