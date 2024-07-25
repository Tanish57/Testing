from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col, when, current_date, lit, substring
from pyspark.sql.types import StructType, StructField, StringType
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CDR Processing") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()

# HDFS input and output paths
input_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/med/2024070316*.cdr"
output_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/output"
processed_files_log_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/processed_files_log.csv"
master_csv_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/trunk/*.csv"
network_node_master_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/network/*.csv"

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
def apply_field_mapping(df, trunk_group_master_df, network_node_master_df):
    df = df.withColumn("EVENT_DIRECTION", when(col("incoming_path") != "", "I")
                                      .when(col("outgoing_path") != "", "O")
                                      .otherwise("T")) \
           .withColumn("INCOMING_POI", when(col("incoming_path") != "", col("trunk_group_master_df.POI"))
                        .otherwise("NULL")) \
           .withColumn("OUTGOING_POI", when(col("outgoing_path") != "", col("trunk_group_master_df.POI"))
                        .otherwise("NULL")) \
           .withColumn("INCOMING_OPERATOR", when(col("incoming_path") != "", col("trunk_group_master_df.OPERATOR"))
                        .otherwise("NULL")) \
           .withColumn("OUTGOING_OPERATOR", when(col("outgoing_path") != "", col("trunk_group_master_df.OPERATOR"))
                        .otherwise("NULL")) \
           .withColumn("FRANCHISE", when(col("incoming_node") != "NULL", col("network_node_master_df.FK_ORGA_FRAN"))
                        .otherwise(col("network_node_master_df.FK_ORGA_FRAN"))) \
           .withColumn("BILLED_PRODUCT", when(col("incoming_product") != "", col("incoming_product"))
                        .otherwise(col("outgoing_product"))) \
           .withColumn("CALL_COUNT", lit("1")) \
           .withColumn("RATING_COMPONENT", lit("TC")) \
           .withColumn("TIER", lit("INTRA")) \
           .withColumn("CURRENCY", lit("INR")) \
           .withColumn("CASH_FLOW", when(col("incoming_path") == "", lit("R"))
                        .otherwise(lit("E"))) \
           .withColumn("ACTUAL_USAGE", substring(col("event_duration"), 1, 4).cast("int") * 3600 + 
                        substring(col("event_duration"), 5, 2).cast("int") * 60 + 
                        substring(col("event_duration"), 7, 2).cast("int")) \
           .withColumn("CHARGED_USAGE", substring(col("event_duration"), 1, 4).cast("int") * 3600 + 
                        substring(col("event_duration"), 5, 2).cast("int") * 60 + 
                        substring(col("event_duration"), 7, 2).cast("int")) \
           .withColumn("CHARGED_UNITS", lit("0")) \
           .withColumn("UNIT_COST_USED", lit("0")) \
           .withColumn("AMOUNT", lit("0")) \
           .withColumn("COMPONENT_DIRECTION", when(col("incoming_path") == "", lit("I"))
                        .when(col("outgoing_path") == "", lit("O"))
                        .otherwise(lit("T"))) \
           .withColumn("FLAT_RATE_CHARGE", lit("0")) \
           .withColumn("PRODUCT_GROUP", lit("TELE")) \
           .withColumn("START_CALL_COUNT", lit("1")) \
           .withColumn("service_type", lit("Access-Voice")) \
           .withColumn("switch_id", when(col("incoming_node") != "", col("incoming_node"))
                        .otherwise(col("outgoing_node"))) \
           .withColumn("trunk_group_id", when(col("incoming_path") != "", col("incoming_path"))
                        .otherwise(col("outgoing_path"))) \
           .withColumn("poi_id", when(col("incoming_path") != "", col("trunk_group_master_df.POI"))
                        .otherwise(col("trunk_group_master_df.POI"))) \
           .withColumn("product_id", when(col("incoming_product") != "", col("incoming_product"))
                        .otherwise(col("outgoing_product"))) \
           .withColumn("network_operator_id", when(col("incoming_path") == "", col("trunk_group_master_df.OPERATOR"))
                        .otherwise(col("trunk_group_master_df.OPERATOR"))) \
           .withColumn("INPUTFILENAME", col("filename")) \
           .withColumn("PROCESSEDTIME", current_date()) \
           .withColumn("SOURCETYPE", lit("MED"))
    
    return df

# Main function to run the job
def main():
    # Get list of already processed files
    processed_files = get_processed_files(processed_files_log_path)
    
    # Read master CSV files
    trunk_group_master_df = spark.read.csv(master_csv_path, header=True).alias("trunk_group_master_df")
    network_node_master_df = spark.read.csv(network_node_master_path, header=True).alias("network_node_master_df")

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

    # Join based on name and employee_name columns
    #joined_df = processed_df.join(df2, df1.name == df2.employee_name, 'inner')
 
    # Select columns from the joined DataFrame
    #result_df = joined_df.select(df1.id, df1.name, df1.age, df2.employee_id, df2.employee_name, df2.salary)
 
    # Show the result
    #result_df.show()
    
    # Join processed data with trunk group master data
    join_columns_trunk_group = ["incoming_path", "ID"]  # Adjust this to the actual columns used for joining
    enriched_df_trunk = join_with_master(processed_df.alias("processed_table"), trunk_group_master_df, join_columns_trunk_group)
    
    # Join processed data with network node master data
    join_columns_network_node = ["incoming_node", "ID"]  # Adjust this to the actual columns used for joining
    enriched_df = join_with_master(enriched_df_trunk, network_node_master_df, join_columns_network_node)

    # Apply field mapping and logic
    final_df = apply_field_mapping(enriched_df, trunk_group_master_df, network_node_master_df)
    
    # Write the enriched data to HDFS in CSV format, partitioned by date
    final_df.write.mode('overwrite').partitionBy("processing_date").csv(output_path, header=True)
    
    # Write the enriched data to a Hive table
    final_df.write.mode('overwrite').saveAsTable("enriched_cdr_data")

    # Update the log with newly processed files
    new_files = new_files_df.select("filename").distinct().rdd.flatMap(lambda x: x).collect()
    update_processed_files(processed_files_log_path, new_files)

if __name__ == "__main__":
    main()
