from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_extract, to_date, when, input_file_name, expr

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CDR Data Processing and Enrichment") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://JioBigDataLakeha") \
    .getOrCreate()

# Define HDFS paths
input_base_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/med/*.cdr"
output_base_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/output/"
trunk_group_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/trunk_group.csv"
network_node_path = "hdfs://JioBigDataLakeha/apps/hive/warehouse/dev_ops.db/ICTPOC/network_node.csv"

# Step 1: Read the raw CDR data
raw_cdr_df = spark.read.option("header", "false").text(input_base_path)

# Apply the transformations
cdr_df = raw_cdr_df \
    .withColumn("incoming_node", when(col("value").substr(1, 20).eqNullSafe(''), 'NULL').otherwise(col("value").substr(1, 20))) \
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
    .withColumn("user_data_3", when(col("value").substr(339, 80).eqNullSafe(''), 'NULL').otherwise(col("value").substr(339, 80))) \
    .withColumn("event_start_date", to_date(col("event_start_date"), "yyyyMMdd")) \
    .withColumn("original_date", regexp_extract(input_file_name(), r'\d{8}', 0))

# Create separate DataFrames for incoming and outgoing nodes
df_incoming = cdr_df.withColumn("outgoing_node", lit(None))
df_outgoing = cdr_df.withColumn("incoming_node", lit(None))

# Union the incoming and outgoing DataFrames
cdr_df = df_incoming.union(df_outgoing).drop("value")

# Step 2: Read and rename columns of the additional data
trunk_group_df = spark.read.option("header", "true").option("sep", "|").csv(trunk_group_path) \
    .withColumnRenamed("ID", "trunk_ID") \
    .withColumnRenamed("NAME", "trunk_NAME") \
    .withColumnRenamed("FK_NNOD", "trunk_FK_NNOD") \
    .withColumnRenamed("OPERATOR", "trunk_OPERATOR") \
    .withColumnRenamed("POI", "trunk_POI")

network_node_df = spark.read.option("header", "true").option("sep", "|").csv(network_node_path) \
    .withColumnRenamed("ID", "network_ID") \
    .withColumnRenamed("NAME", "network_NAME") \
    .withColumnRenamed("FK_ORGA_FRAN", "network_FK_ORGA_FRAN")

# Step 3: Perform lookups and enrich the main DataFrame
cdr_df = cdr_df \
    .join(trunk_group_df.withColumnRenamed("trunk_ID", "incoming_node"), 
          (cdr_df.incoming_path == trunk_group_df.trunk_ID) & (cdr_df.incoming_node == trunk_group_df.trunk_FK_NNOD), "left") \
    .withColumnRenamed("trunk_POI", "INCOMING_POI") \
    .withColumnRenamed("trunk_OPERATOR", "INCOMING_OPERATOR") \
    .drop("trunk_ID", "trunk_NAME", "trunk_FK_NNOD")

cdr_df = cdr_df \
    .join(trunk_group_df.withColumnRenamed("trunk_ID", "outgoing_node"), 
          (cdr_df.outgoing_path == trunk_group_df.trunk_ID) & (cdr_df.outgoing_node == trunk_group_df.trunk_FK_NNOD), "left") \
    .withColumnRenamed("trunk_POI", "OUTGOING_POI") \
    .withColumnRenamed("trunk_OPERATOR", "OUTGOING_OPERATOR") \
    .drop("trunk_ID", "trunk_NAME", "trunk_FK_NNOD")

cdr_df = cdr_df \
    .withColumn("FRANCHISE", when(col("incoming_node").isNotNull(), col("incoming_node")).otherwise(col("outgoing_node"))) \
    .join(network_node_df, col("FRANCHISE") == network_node_df.network_ID, "left") \
    .withColumn("FRANCHISE", col("network_FK_ORGA_FRAN")) \
    .drop("network_ID", "network_NAME", "network_FK_ORGA_FRAN")

cdr_df = cdr_df \
    .withColumn("BILLING_OPERATOR", when(col("incoming_node").isNotNull(), col("INCOMING_OPERATOR")).otherwise(col("OUTGOING_OPERATOR"))) \
    .withColumn("BILLED_PRODUCT", when(col("incoming_node").isNotNull(), col("incoming_product")).otherwise(col("outgoing_product"))) \
    .withColumn("CASH_FLOW", when(col("incoming_path").isNotNull(), lit("R")).otherwise(lit("E"))) \
    .withColumn("COMPONENT_DIRECTION", expr("""
        CASE 
            WHEN incoming_path IS NOT NULL THEN 'I'
            WHEN outgoing_path IS NOT NULL THEN 'O'
            WHEN outgoing_path IS NOT NULL AND incoming_path IS NOT NULL THEN 'T'
        END
    """)) \
    .withColumn("SWITCH_ID", when(col("incoming_node").isNotNull(), col("incoming_node")).otherwise(col("outgoing_node"))) \
    .withColumn("trunk_group_id", when(col("incoming_path").isNotNull(), col("incoming_path")).otherwise(col("outgoing_path"))) \
    .withColumn("poi_id", when(col("incoming_path").isNotNull(), col("INCOMING_POI")).otherwise(col("OUTGOING_POI"))) \
    .withColumn("product_id", when(col("incoming_path").isNotNull(), col("incoming_path")).otherwise(col("outgoing_path"))) \
    .withColumn("network_operator_id", when(col("incoming_path").isNotNull(), col("INCOMING_OPERATOR")).otherwise(col("OUTGOING_OPERATOR")))

# Step 4: Write the final enriched DataFrame to HDFS
cdr_df.write.mode("overwrite").csv(output_base_path)

# Stop the Spark session
spark.stop()
