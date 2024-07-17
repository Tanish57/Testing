from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col, when, current_date, lit
from pyspark.sql.types import StructType, StructField, StringType
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CDR Processing") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .enableHiveSupport() \
    .getOrCreate()

# HDFS input and output paths
input_path = "hdfs://JBDLha/Apps/warehouse/invotp/med/*.cdr"
output_path = "hdfs://JBDLha/Apps/warehouse/invotp/processed/"
processed_files_log_path = "hdfs://JBDLha/Apps/warehouse/invotp/processed_files_log.csv"
master_csv_path = "hdfs://JBDLha/Apps/warehouse/master/master.csv"

# Schema for the processed files log
log_schema = StructType([
    StructField("filename", StringType(), True)
])

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

# Function to process CDR files
def process_cdr_data(df):
    # Convert fixed-length records to DataFrame with specified schema and handle NULL replacements
    df = df.withColumn("incoming_node", when(col("value").substr(1, 20).eqNullSafe(''), 'NULL').otherwise(col("value").substr(1, 20))) \
           .withColumn("outgoing_node", when(col("value").substr(21, 20).eqNullSafe(''), 'NULL').otherwise(col("value").substr(21, 20))) \
           .withColumn("event_start_date", when(col("value").substr(41, 8).eqNullSafe(''), 'NULL').otherwise(col("value").substr(41, 8))) \
           .withColumn("event_start_time", when(col("value").substr(49, 8).eqNullSafe(''), 'NULL').otherwise(col("value").substr(49, 8))) \
           .withColumn("event_duration", when(col("value").substr(57, 10).eqNullSafe(''), 'NULL').otherwise(col("value").substr(57, 10))) \
           .withColumn("anum", when(col("value").substr(67, 28).eqNullSafe(''), 'NULL').otherwise(col("value").substr(67, 28))) \
           .withColumn("bnum", when(col("value").substr(95, 28).eqNullSafe(''), 'NULL').otherwise(col("value").substr(95, 28))) \
           .withColumn("incoming_path", when(col("value").substr(123, 20).eqNullSafe(''), 'NULL').otherwise(col("value").substr(123, 20))) \
           .withColumn("outgoing_path", when(col("value").substr(143, 20).eqNullSafe(''), 'NULL').otherwise(col("value").substr(143, 20))) \
           .withColumn("incoming_product", when(col("value").substr(163, 14).eqNullSafe(''), 'NULL').otherwise(col("value").substr(163, 14))) \
           .withColumn("outgoing_product", when(col("value").substr(177, 14).eqNullSafe(''), 'NULL').otherwise(col("value").substr(177, 14))) \
           .withColumn("event_direction", when(col("value").substr(191, 1).eqNullSafe(''), 'NULL').otherwise(col("value").substr(191, 1))) \
           .withColumn("discrete_rating_parameter_1", when(col("value").substr(192, 15).eqNullSafe(''), 'NULL').otherwise(col("value").substr(192, 15))) \
           .withColumn("data_unit", when(col("value").substr(207, 8).eqNullSafe(''), 'NULL').otherwise(col("value").substr(207, 8))) \
           .withColumn("record_sequence_number", when(col("value").substr(215, 40).eqNullSafe(''), 'NULL').otherwise(col("value").substr(215, 40))) \
           .withColumn("record_type", when(col("value").substr(255, 2).eqNullSafe(''), 'NULL').otherwise(col("value").substr(255, 2))) \
           .withColumn("user_summarisation", when(col("value").substr(257, 20).eqNullSafe(''), 'NULL').otherwise(col("value").substr(257, 20))) \
           .withColumn("user_data", when(col("value").substr(277, 30).eqNullSafe(''), 'NULL').otherwise(col("value").substr(277, 30))) \
           .withColumn("user_data_2", when(col("value").substr(307, 30).eqNullSafe(''), 'NULL').otherwise(col("value").substr(307, 30))) \
           .withColumn("link_field", when(col("value").substr(337, 2).eqNullSafe(''), 'NULL').otherwise(col("value").substr(337, 2))) \
           .withColumn("user_data_3", when(col("value").substr(339, 80).eqNullSafe(''), 'NULL').otherwise(col("value").substr(339, 80)))
    
    # Drop the original value column
    df = df.drop("value")
    
    return df

# Function to join with master CSV
def join_with_master(processed_df, master_df, join_columns):
    # Join processed data with master data
    enriched_df = processed_df.join(master_df, on=join_columns, how='left')
    return enriched_df

# Function to apply field mapping and logic for the final hive table
def apply_field_mapping(df):
    df = df.withColumn("FRANCHISE", when(col("incoming_node") == "X", "Franchise X")
                       .when(col("outgoing_node") == "Y", "Franchise Y")
                       .otherwise("Franchise Default")) \
           .withColumn("BILLED_PRODUCT", when(col("incoming_product") == "A", "Product A")
                       .otherwise("Product Default")) \
           # Have to add more columns and logic based on mapping
    
    return df

# Main function to run the job
def main():
    # Get list of already processed files
    processed_files = get_processed_files(processed_files_log_path)
    
    # Read master CSV file
    master_df = spark.read.csv(master_csv_path, header=True)

    # Read new files from HDFS
    raw_df = spark.read.text(input_path).withColumn("filename", input_file_name())
    
    # Filter out already processed files
    new_files_df = raw_df.filter(~col("filename").isin(processed_files))
    
    if new_files_df.count() == 0:
        print("No new files to process")
        return
    
    # Process the new files
    processed_df = process_cdr_data(new_files_df)
    
    # Add a date column to partition the output by date
    processed_df = processed_df.withColumn("processing_date", current_date())

    # Join processed data with master CSV data
    join_columns = ["incoming_node"]  # Adjust this to the actual columns used for joining
    enriched_df = join_with_master(processed_df, master_df, join_columns)

    # Apply field mapping and logic
    final_df = apply_field_mapping(enriched_df)
    
    # Write the enriched data to HDFS in CSV format, partitioned by date
    final_df.write.mode('overwrite').partitionBy("processing_date").csv(output_path, header=True)
    
    # Write the enriched data to a Hive table
    final_df.write.mode('overwrite').saveAsTable("enriched_cdr_data")

    # Update the log with newly processed files
    new_files = new_files_df.select("filename").distinct().rdd.flatMap(lambda x: x).collect()
    update_processed_files(processed_files_log_path, new_files)

if __name__ == "__main__":
    main()
