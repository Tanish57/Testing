from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col, lit, when, substring, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import os

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("CSV Processing") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .enableHiveSupport() \
    .getOrCreate()

# HDFS input and output paths
input_path = "/Documents/Python/PySpark/CSV/"
processed_files_log_path = "/Documents/Python/PySpark/CSV/processed_csv_log.csv"
trunk_group_master_path = "/Documents/Python/PySpark/CSV/Trunk_Group_Master.csv"

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
def process_csv_data(file_path, trunk_group_master):
    # Read the file, skipping the first three rows
    raw_rdd = spark.sparkContext.textFile(file_path).zipWithIndex().filter(lambda x: x[1] >= 3).keys()
    df = spark.read.csv(raw_rdd, header=True)

    # Extract the processing date from the metadata row
    first_row = spark.read.csv(file_path, header=False).head()
    processing_date = first_row[4][:8]  # Extract date in 'YYYYMMDD' format

    # Select the required columns and replace blank values with 'NULL'
    for column in required_columns:
        df = df.withColumn(column, when(col(column) == '', 'NULL').otherwise(col(column)))
    
    # Applying specific transformations
    df = df.withColumn("EVENT_START_TIME", substring(col("EVENT_START_TIME"), 1, 6))
    df = df.withColumn("service_type", lit("Access-Voice"))
    df = df.withColumn("switch_id", when(col("INCOMING_NODE") != "", col("INCOMING_NODE")).otherwise(col("OUTGOING_NODE")))
    df = df.withColumn("trunk_group_id", when(col("INCOMING_PATH") != "", col("INCOMING_PATH")).otherwise(col("OUTGOING_PATH")))
    df = df.withColumn("poi_id", when(col("INCOMING_PATH") != "", 
                                      col("INCOMING_PATH")).otherwise(col("OUTGOING_PATH"))) \
           .join(trunk_group_master, (col("INCOMING_PATH") == trunk_group_master["ID"]) & (col("INCOMING_NODE") == trunk_group_master["FK_NNOD"]), "left") \
           .select("POI")
    df = df.withColumn("product_id", when(col("INCOMING_PRODUCT") != "", col("INCOMING_PRODUCT")).otherwise(col("OUTGOING_PRODUCT")))
    df = df.withColumn("network_operator_id", when(col("INCOMING_PATH") != "", 
                                                   col("INCOMING_PATH")).otherwise(col("OUTGOING_PATH"))) \
           .join(trunk_group_master, (col("INCOMING_PATH") == trunk_group_master["ID"]) & (col("INCOMING_NODE") == trunk_group_master["FK_NNOD"]), "left") \
           .select("OPERATOR")
    df = df.withColumn("INPUTFILENAME", lit(os.path.basename(file_path)))
    df = df.withColumn("PROCESSEDTIME", current_timestamp())
    df = df.withColumn("SOURCETYPE", lit("ICT"))
    
    df = df.withColumn("processing_date", lit(processing_date))
    
    return df

# Main function to run the job
def main():
    # Get list of already processed files
    processed_files = get_processed_files(processed_files_log_path)
    
    # Read the trunk group master file
    trunk_group_master = spark.read.csv(trunk_group_master_path, header=True)
    
    # Read new files from HDFS
    raw_df = spark.read.csv(input_path, header=True).withColumn("filename", input_file_name())
    
    # Filter out already processed files
    new_files_df = raw_df.filter(~col("filename").isin(processed_files))
    
    if new_files_df.count() == 0:
        print("No new files to process")
        return
    
    # Process the new files
    processed_dfs = []
    for row in new_files_df.select("filename").distinct().collect():
        file_path = row.filename
        processed_dfs.append(process_csv_data(file_path, trunk_group_master))
    
    processed_df = spark.union(processed_dfs)
    
    # Write the processed data to Hive table, partitioned by the extracted date
    processed_df.write.mode('append').partitionBy("processing_date").saveAsTable("processed_data_table")

    # Update the log with newly processed files
    new_files = new_files_df.select("filename").distinct().rdd.flatMap(lambda x: x).collect()
    update_processed_files(processed_files_log_path, new_files)

if __name__ == "__main__":
    main()
