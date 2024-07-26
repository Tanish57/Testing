from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col, when, current_date, lit, substring, to_date, regexp_extract
import logging

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CDR Processing Test") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# HDFS paths for input and output
input_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/med/2024070316/*.cdr"
output_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/output_test/"
trunk_group_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/trunk/*.csv"
network_node_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/network/*.csv"

# Function to process CDR files
def process_cdr_data(df):
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

# Function to join with master CSV using explicit join condition
def join_with_master(processed_df, master_df, join_condition, master_columns):
    # Join processed data with master data using the provided join condition
    enriched_df = processed_df.join(master_df.select(*master_columns), join_condition, how='left')
    return enriched_df

# Function to apply field mapping and logic for the final hive table
def apply_field_mapping(df):
    df = df.withColumn("EVENT_DIRECTION", when(col("incoming_path") != "", "I")
                                      .when(col("outgoing_path") != "", "O")
                                      .otherwise("T")) \
           .withColumn("INCOMING_POI", when(col("incoming_path") != "", col("trunk_group_POI"))
                        .otherwise("NULL")) \
           .withColumn("OUTGOING_POI", when(col("outgoing_path") != "", col("trunk_group_POI"))
                        .otherwise("NULL")) \
           .withColumn("INCOMING_OPERATOR", when(col("incoming_path") != "", col("trunk_group_OPERATOR"))
                        .otherwise("NULL")) \
           .withColumn("OUTGOING_OPERATOR", when(col("outgoing_path") != "", col("trunk_group_OPERATOR"))
                        .otherwise("NULL")) \
           .withColumn("FRANCHISE", when(col("incoming_node") != "NULL", col("network_node_FRANCHISE"))
                        .otherwise(col("network_node_FRANCHISE"))) \
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
           .withColumn("poi_id", when(col("incoming_path") != "", col("trunk_group_POI"))
                        .otherwise(col("trunk_group_POI"))) \
           .withColumn("product_id", when(col("incoming_product") != "", col("incoming_product"))
                        .otherwise(col("outgoing_product"))) \
           .withColumn("network_operator_id", when(col("incoming_path") == "", col("trunk_group_OPERATOR"))
                        .otherwise(col("trunk_group_OPERATOR"))) \
           .withColumn("INPUTFILENAME", col("filename")) \
           .withColumn("PROCESSEDTIME", current_date()) \
           .withColumn("SOURCETYPE", lit("MED"))
   
    return df

# Main function to run the job
def main():
    # Read all trunk group and network node CSV files
    trunk_group_master_df = spark.read.csv(trunk_group_path, header=True, sep='|').alias("trunk_group_master_df")
    network_node_master_df = spark.read.csv(network_node_path, header=True, sep='|').alias("network_node_master_df")

    # Log the columns of trunk group and network node master DataFrames
    logger.info(f"Trunk Group Master DataFrame Columns: {trunk_group_master_df.columns}")
    logger.info(f"Network Node Master DataFrame Columns: {network_node_master_df.columns}")

    # Read the CDR files
    raw_df = spark.read.text(input_path).withColumn("filename", input_file_name())

    # Process the CDR data
    processed_df = process_cdr_data(raw_df)

    # Add a processing date column
    processed_df = processed_df.withColumn("processing_date", current_date())

    # Join processed data with trunk group master data using explicit join condition
    trunk_group_join_condition = processed_df["incoming_path"] == trunk_group_master_df["ID"]
    enriched_df_trunk = join_with_master(processed_df, trunk_group_master_df, trunk_group_join_condition, ["ID", "POI", "OPERATOR"])

    # Select necessary columns to avoid duplicates
    enriched_df_trunk = enriched_df_trunk.select(
        processed_df["*"],
        col("POI").alias("trunk_group_POI"),
        col("OPERATOR").alias("trunk_group_OPERATOR")
    )

    # Join processed data with network node master data using explicit join condition
    network_node_join_condition = enriched_df_trunk["incoming_node"] == network_node_master_df["ID"]
    enriched_df = join_with_master(enriched_df_trunk, network_node_master_df, network_node_join_condition, ["ID", "FK_ORGA_FRAN"])

    # Select necessary columns to avoid duplicates
    enriched_df = enriched_df.select(
        enriched_df_trunk["*"],
        col("FK_ORGA_FRAN").alias("network_node_FRANCHISE")
    )

    # Apply field mapping and logic
    final_df = apply_field_mapping(enriched_df)

    # Write the enriched data to HDFS in CSV format, partitioned by date
    final_df.write.mode('overwrite').partitionBy("processing_date").csv(output_path, header=True)

if __name__ == "__main__":
    main()
