from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col, lit, when, substring, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import os

# Kerberos principal and keytab file
principal = "your_principal@YOUR.REALM.COM"
keytab = "/path/to/your.keytab"

# Initialize Spark session with Kerberos authentication
spark = SparkSession.builder \
    .appName("CSV Processing with Kerberos") \
    .config("spark.yarn.principal", principal) \
    .config("spark.yarn.keytab", keytab) \
    .config("spark.hadoop.security.authentication", "kerberos") \
    .config("spark.hadoop.security.authorization", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# HDFS input and output paths
input_path = "hdfs://path/to/input/"  # Replace with your HDFS input path
output_path = "hdfs://path/to/output/"  # Replace with your HDFS output path
hold_path = "hdfs://path/to/hold/"  # Replace with your HDFS hold path for unmatched records
processed_files_log_path = "hdfs://path/to/processed_files_log.csv"  # Replace with your HDFS log path
trunk_group_master_base_path = "hdfs://path/to/trunk_group_master/"  # Replace with your HDFS trunk group master base path

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
    # Read the file, skipping the first and last two rows (metadata)
    raw_rdd = spark.sparkContext.textFile(file_path).zipWithIndex().filter(lambda x: x[1] >= 1 and x[1] < raw_rdd.count() - 2).keys()
    df = spark.read.csv(raw_rdd, header=True)

    # Select the required columns and replace blank values with 'NULL'
    for column in required_columns:
        df = df.withColumn(column, when(col(column) == '', 'NULL').otherwise(col(column)))

    # Applying specific transformations
    df = df.withColumn("EVENT_START_TIME", substring(col("EVENT_START_TIME"), 1, 6))
    df = df.withColumn("service_type", lit("Access-Voice"))
    df = df.withColumn("switch_id", when(col("INCOMING_NODE") != "", col("INCOMING_NODE")).otherwise(col("OUTGOING_NODE")))
    df = df.withColumn("trunk_group_id", when(col("INCOMING_PATH") != "", col("INCOMING_PATH")).otherwise(col("OUTGOING_PATH")))

    # Handle incoming_node and outgoing_node separately
    incoming_df = df.filter(col("INCOMING_NODE") != "NULL")
    outgoing_df = df.filter(col("OUTGOING_NODE") != "NULL")
    outgoing_df = outgoing_df.withColumnRenamed("OUTGOING_NODE", "INCOMING_NODE") \
                             .withColumnRenamed("OUTGOING_PATH", "INCOMING_PATH") \
                             .withColumnRenamed("OUTGOING_PRODUCT", "INCOMING_PRODUCT")

    df = incoming_df.union(outgoing_df)

    df = df.withColumn("poi_id", when(col("INCOMING_PATH") != "", col("INCOMING_PATH")).otherwise(col("OUTGOING_PATH"))) \
           .join(trunk_group_master, (col("INCOMING_PATH") == trunk_group_master["ID"]) & (col("INCOMING_NODE") == trunk_group_master["FK_NNOD"]), "left") \
           .select("POI")
    df = df.withColumn("product_id", when(col("INCOMING_PRODUCT") != "", col("INCOMING_PRODUCT")).otherwise(col("OUTGOING_PRODUCT")))
    df = df.withColumn("network_operator_id", when(col("INCOMING_PATH") != "", col("INCOMING_PATH")).otherwise(col("OUTGOING_PATH"))) \
           .join(trunk_group_master, (col("INCOMING_PATH") == trunk_group_master["ID"]) & (col("INCOMING_NODE") == trunk_group_master["FK_NNOD"]), "left") \
           .select("OPERATOR")
    df = df.withColumn("INPUTFILENAME", lit(os.path.basename(file_path)))
    df = df.withColumn("PROCESSEDTIME", current_timestamp())
    df = df.withColumn("SOURCETYPE", lit("ICT"))

    df = df.withColumn("processing_date", lit(processing_date))

    # Deduplicate based on key columns (e.g., ANUM, BNUM, EVENT_START_DATE, EVENT_START_TIME)
    deduped_df = df.dropDuplicates(["ANUM", "BNUM", "EVENT_START_DATE", "EVENT_START_TIME"])

    # Split matched and unmatched records
    matched_df = deduped_df.filter(col("POI").isNotNull() & col("OPERATOR").isNotNull())
    unmatched_df = deduped_df.filter(col("POI").isNull() | col("OPERATOR").isNull())

    return matched_df, unmatched_df

# Main function to run the job
def main():
    # Get list of already processed files
    processed_files = get_processed_files(processed_files_log_path)

    # Read the trunk group master file
    latest_master_folder = sorted(os.listdir(trunk_group_master_base_path))[-1]
    trunk_group_master_path = os.path.join(trunk_group_master_base_path, latest_master_folder, "Trunk_Group_Master.csv")
    trunk_group_master = spark.read.csv(trunk_group_master_path, header=True)

    # Read new files from HDFS
    raw_df = spark.read.csv(input_path, header=True).withColumn("filename", input_file_name())

    # Filter out already processed files
    new_files_df = raw_df.filter(~col("filename").isin(processed_files))

    if new_files_df.count() == 0:
        print("No new files to process")
        return

    # Process the new files
    for row in new_files_df.select("filename").distinct().collect():
        file_path = row.filename
        matched_df, unmatched_df = process_csv_data(file_path, trunk_group_master)

        # Write the matched data to Hive table, partitioned by the extracted date
        spark.sql("CREATE TABLE IF NOT EXISTS processed_data_table (" +
                  ", ".join([f"{col} STRING" for col in required_columns]) +
                  ") PARTITIONED BY (processing_date STRING) STORED AS TEXTFILE")

        matched_df.write.mode('append').partitionBy("processing_date").saveAsTable("processed_data_table")

        # Write the matched data to HDFS in CSV format, partitioned by the extracted date
        output_file_path = os.path.join(output_path, f"{os.path.basename(file_path)}")
        matched_df.write.mode('overwrite').partitionBy("processing_date").csv(output_file_path, header=True)

        # Write the unmatched data to hold folder
        hold_file_path = os.path.join(hold_path, f"{os.path.basename(file_path)}")
        unmatched_df.write.mode('overwrite').csv(hold_file_path, header=True)

        # Update the log with newly processed file
        update_processed_files(processed_files_log_path, [file_path])

if __name__ == "__main__":
    main()
