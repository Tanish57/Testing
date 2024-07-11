from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col
from pyspark.sql.types import StructType, StructField, StringType
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CDR Processing") \
    .getOrCreate()

# HDFS input and output paths
input_path = "hdfs://JBDLha/Apps/warehouse/invotp/med/"
output_path = "hdfs://JBDLHa/Apps/warehouse/invotp/processed/"
processed_files_log_path = "hdfs://JBDLha/Apps/warehouse/invotp/processed_files_log.csv"

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
    # Define the schema of the CDR file with 20 fields (adjust the schema according to your CDR file format)
    schema = StructType([
        StructField("Field1", StringType(), True),
        StructField("Field2", StringType(), True),
        StructField("Field3", StringType(), True),
        StructField("Field4", StringType(), True),
        StructField("Field5", StringType(), True),
        StructField("Field6", StringType(), True),
        StructField("Field7", StringType(), True),
        StructField("Field8", StringType(), True),
        StructField("Field9", StringType(), True),
        StructField("Field10", StringType(), True),
        StructField("Field11", StringType(), True),
        StructField("Field12", StringType(), True),
        StructField("Field13", StringType(), True),
        StructField("Field14", StringType(), True),
        StructField("Field15", StringType(), True),
        StructField("Field16", StringType(), True),
        StructField("Field17", StringType(), True),
        StructField("Field18", StringType(), True),
        StructField("Field19", StringType(), True),
        StructField("Field20", StringType(), True)
    ])
    
    # Convert fixed-length records to DataFrame with specified schema
    df = df.withColumn("Field1", col("value").substr(1, 21)) \
           .withColumn("Field2", col("value").substr(22, 20)) \
           .withColumn("Field3", col("value").substr(42, 8)) \
           .withColumn("Field4", col("value").substr(50, 8)) \
           .withColumn("Field5", col("value").substr(58, 10)) \
           .withColumn("Field6", col("value").substr(68, 28)) \
           .withColumn("Field7", col("value").substr(96, 28)) \
           .withColumn("Field8", col("value").substr(124, 20)) \
           .withColumn("Field9", col("value").substr(144, 20)) \
           .withColumn("Field10", col("value").substr(164, 14)) \
           .withColumn("Field11", col("value").substr(178, 14)) \
           .withColumn("Field12", col("value").substr(192, 1)) \
           .withColumn("Field13", col("value").substr(193, 15)) \
           .withColumn("Field14", col("value").substr(208, 8)) \
           .withColumn("Field15", col("value").substr(216, 40)) \
           .withColumn("Field16", col("value").substr(256, 2)) \
           .withColumn("Field17", col("value").substr(258, 20)) \
           .withColumn("Field18", col("value").substr(278, 30)) \
           .withColumn("Field19", col("value").substr(308, 30)) \
           .withColumn("Field20", col("value").substr(338, 2))
    
    # Drop the original value column
    df = df.drop("value")
    
    return df

# Main function to run the job
def main():
    # Get list of already processed files
    processed_files = get_processed_files(processed_files_log_path)
    
    # Read new files from HDFS
    raw_df = spark.read.text(input_path).withColumn("filename", input_file_name())
    
    # Filter out already processed files
    new_files_df = raw_df.filter(~col("filename").isin(processed_files))
    
    if new_files_df.count() == 0:
        print("No new files to process")
        return
    
    # Process the new files
    processed_df = process_cdr_data(new_files_df)
    
    # Write the processed data to HDFS in CSV format
    processed_df.write.mode('overwrite').csv(output_path, header=True)
    
    # Update the log with newly processed files
    new_files = new_files_df.select("filename").distinct().rdd.flatMap(lambda x: x).collect()
    update_processed_files(processed_files_log_path, new_files)

if __name__ == "__main__":
    main()
