from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col, current_date
from pyspark.sql.types import StructType, StructField, StringType
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV Processing") \
    .getOrCreate()

# HDFS input and output paths
input_path = "hdfs://JBDLha/Apps/warehouse/csv_data/"
output_path = "hdfs://JBDLha/Apps/warehouse/processed_csv/"
processed_files_log_path = "hdfs://JBDLha/Apps/warehouse/processed_csv_log.csv"

# Schema for the processed files log
log_schema = StructType([
    StructField("filename", StringType(), True)
])

# Required columns to select
required_columns = [
    "INCOMING_NODE", "OUTGOING_NODE", "EVENT_START_DATE", "EVENT_START_TIME", "EVENT_DURATION",
    "ANUM", "BNUM", "INCOMING_PATH", "OUTGOING_PATH", "INCOMING_PRODUCT", "OUTGOING_PRODUCT",
    "EVENT_DIRECTION", "INCOMING_POI", "OUTGOING_POI", "INCOMING_OPERATOR", "OUTGOING_OPERATOR",
    "FRANCHISE", "BILLING_OPERATOR", "BILLED_PRODUCT", "CALL_COUNT", "RATING_COMPONENT", "TIER",
    "CURRENCY", "CASH_FLOW", "ACTUAL_USAGE", "CHARGED_USAGE", "CHARGED_UNITS", "UNIT_COST_USED",
    "AMOUNT", "COMPONENT_DIRECTION", "FLAT_RATE_CHARGE", "PRODUCT_GROUP", "START_CALL_COUNT",
    "service_type", "switch_id", "trunk_group_id", "poi_id", "product_id", "network_operator_id",
    "INPUTFILENAME", "PROCESSEDTIME", "SOURCETYPE"
]

# Function to read the log of processed files
def get_processed_files(log_path):
    if not os.path.exists(log_path):
        return set()
    df = spark.read.csv(log_path, schema=log_schema, header=True)
    return set(df.select("filename").rdd.flatMap(lambda x: x).collect())

# Function to update the log of processed files
def update_processed_files(log_path, files):
    new_log = spark.createDataFrame([(file,) for file in files], ["filename"])
    if os.path.exists(log_path):
        existing_log = spark.read.csv(log_path, schema=log_schema, header=True)
        updated_log = existing_log.union(new_log).distinct()
    else:
        updated_log = new_log
    updated_log.write.mode('overwrite').csv(log_path, header=True)

# Function to process CSV files
def process_csv_data(df):
    df = df.select(*required_columns)
    return df

# Main function to run the job
def main():
    # Get list of already processed files
    processed_files = get_processed_files(processed_files_log_path)
    
    # Read new files from HDFS
    raw_df = spark.read.csv(input_path, header=True).withColumn("filename", input_file_name())
    
    # Filter out already processed files
    new_files_df = raw_df.filter(~col("filename").isin(processed_files))
    
    if new_files_df.count() == 0:
        print("No new files to process")
        return
    
    # Process the new files
    processed_df = process_csv_data(new_files_df)
    
    # Add a date column to partition the output by date
    processed_df = processed_df.withColumn("processing_date", current_date())
    
    # Write the processed data to HDFS in CSV format, partitioned by date
    processed_df.write.mode('overwrite').partitionBy("processing_date").csv(output_path, header=True)
    
    # Update the log with newly processed files
    new_files = new_files_df.select("filename").distinct().rdd.flatMap(lambda x: x).collect()
    update_processed_files(processed_files_log_path, new_files)

if __name__ == "__main__":
    main()
