from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col, when, current_date, lit, substring, to_date, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType
import os
import shutil

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CDR Processing") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()

# HDFS paths
input_base_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/med/"
output_base_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/output/"
processed_files_log_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/processed_files_log.csv"
trunk_group_base_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/trunk/"
network_node_base_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/network/"
hold_folder_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/hold/"

# Schema for the processed files log
log_schema = StructType([
    StructField("filename", StringType(), True),
    StructField("original_date", StringType(), True)
])

# Function to read the log of processed files
def get_processed_files(log_path):
    if not os.path.exists(log_path):
        return set()
    df = spark.read.csv(log_path, schema=log_schema, header=True)
    return set(df.select("filename").rdd.flatMap(lambda x: x).collect())

# Function to update the log of processed files
def update_processed_files(log_path, files, original_date):
    new_log = spark.createDataFrame([(file, original_date) for file in files], ["filename", "original_date"])
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
    
    # Convert event_start_date to date format
    df = df.withColumn("event_start_date", to_date(col("event_start_date"), "yyyyMMdd"))
    
    # Extract original processing date from the filename
    df = df.withColumn("original_date", regexp_extract(col("filename"), r'\d{8}', 0))
    
    # Split the records where both incoming_node and outgoing_node are not NULL into two records
    df_incoming = df.withColumn("outgoing_node", lit(None))
    df_outgoing = df.withColumn("incoming_node", lit(None))
    
    # Union the two dataframes to get separate records
    df = df_incoming.union(df_outgoing)
    
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
           .withColumn("CASH_FLOW", when(col("incoming_path") == "", lit("
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

# Function to move unmatched records to hold folder
def move_to_hold_folder(df, hold_folder_path, filename_col='filename'):
    hold_files = df.select(filename_col).distinct().collect()
    for row in hold_files:
        file_to_move = row[filename_col]
        # Assuming HDFS command to move file to hold folder
        os.system(f'hdfs dfs -mv {file_to_move} {hold_folder_path}')

# Function to process files from hold folder
def process_hold_files(hold_folder_path, trunk_group_master_df, network_node_master_df, processed_files):
    hold_files = [os.path.join(hold_folder_path, f) for f in os.listdir(hold_folder_path)]
    if hold_files:
        raw_df = spark.read.text(hold_files).withColumn("filename", input_file_name())
        new_files_df = raw_df.filter(~col("filename").isin(processed_files))
        if new_files_df.count() > 0:
            processed_df = process_cdr_data(new_files_df)
            processed_df = processed_df.withColumn("processing_date", current_date())
            join_columns_trunk_group = ["incoming_path", "ID"]
            enriched_df_trunk = join_with_master(processed_df.alias("processed_table"), trunk_group_master_df, join_columns_trunk_group)
            join_columns_network_node = ["incoming_node", "ID"]
            enriched_df = join_with_master(enriched_df_trunk, network_node_master_df, join_columns_network_node)
            matched_df = enriched_df.filter(col("trunk_group_master_df.ID").isNotNull() & col("network_node_master_df.ID").isNotNull())
            unmatched_df = enriched_df.filter(col("trunk_group_master_df.ID").isNull() | col("network_node_master_df.ID").isNull())
            if unmatched_df.count() > 0:
                move_to_hold_folder(unmatched_df, hold_folder_path)
            final_df = apply_field_mapping(matched_df, trunk_group_master_df, network_node_master_df)
            for row in matched_df.select("original_date").distinct().collect():
                original_date = row["original_date"]
                output_path = os.path.join(output_base_path, original_date)
                final_df.filter(col("original_date") == original_date).write.mode('overwrite').partitionBy("processing_date").csv(output_path, header=True)
            new_files = new_files_df.select("filename").distinct().rdd.flatMap(lambda x: x).collect()
            update_processed_files(processed_files_log_path, new_files, original_date)

# Main function to run the job
def main():
    processed_files = get_processed_files(processed_files_log_path)
    trunk_group_latest_path = max([os.path.join(trunk_group_base_path, d) for d in os.listdir(trunk_group_base_path)], key=os.path.getmtime)
    network_node_latest_path = max([os.path.join(network_node_base_path, d) for d in os.listdir(network_node_base_path)], key=os.path.getmtime)
    trunk_group_master_df = spark.read.csv(trunk_group_latest_path, header=True, sep='|').alias("trunk_group_master_df")
    network_node_master_df = spark.read.csv(network_node_latest_path, header=True, sep='|').alias("network_node_master_df")

    # Process each hourly directory of input files
    for hour_dir in sorted([os.path.join(input_base_path, d) for d in os.listdir(input_base_path)]):
        raw_df = spark.read.text(hour_dir + "/*.cdr").withColumn("filename", input_file_name())
        new_files_df = raw_df.filter(~col("filename").isin(processed_files))
        if new_files_df.count() == 0:
            print(f"No new files to process in {hour_dir}")
            continue
        processed_df = process_cdr_data(new_files_df)
        processed_df = processed_df.withColumn("processing_date", current_date())
        join_columns_trunk_group = ["incoming_path", "ID"]
        enriched_df_trunk = join_with_master(processed_df.alias("processed_table"), trunk_group_master_df, join_columns_trunk_group)
        join_columns_network_node = ["incoming_node", "ID"]
        enriched_df = join_with_master(enriched_df_trunk, network_node_master_df, join_columns_network_node)
        matched_df = enriched_df.filter(col("trunk_group_master_df.ID").isNotNull() & col("network_node_master_df.ID").isNotNull())
        unmatched_df = enriched_df.filter(col("trunk_group_master_df.ID").isNull() | col("network_node_master_df.ID").isNull())
        if unmatched_df.count() > 0:
            move_to_hold_folder(unmatched_df, hold_folder_path)
        final_df = apply_field_mapping(matched_df, trunk_group_master_df, network_node_master_df)
        for row in matched_df.select("original_date").distinct().collect():
            original_date = row["original_date"]
            output_path = os.path.join(output_base_path, original_date)
            final_df.filter(col("original_date") == original_date).write.mode('overwrite').partitionBy("processing_date").csv(output_path, header=True)
        new_files = new_files_df.select("filename").distinct().rdd.flatMap(lambda x: x).collect()
        update_processed_files(processed_files_log_path, new_files, original_date)

    # Process hold folder
    process_hold_files(hold_folder_path, trunk_group_master_df, network_node_master_df, processed_files)

if __name__ == "__main__":
    main()
