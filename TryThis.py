from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import os
os.environ['PYSPARK_PYTHON']
 
# Initialize Spark session
spark = SparkSession.builder \
    .appName("CDRProcessing") \
    .config("spark.driver.host", "localhost") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "100s") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
 
 
 
# Define the column names and their fixed lengths
columns = [
    "incoming_node", "outgoing_node", "event_start_date", "event_start_time", "event_duration",
    "anum", "bnum", "incoming_path", "outgoing_path", "incoming_product", "outgoing_product",
    "event_direction", "discrete_rating_parameter_1", "data_unit", "record_sequence_number",
    "record_type", "user_summarisation", "user_data", "user_data_2", "link_field", "user_data_3"
]
 
lengths = [
    20, 20, 8, 8, 10,
    28, 28, 20, 20, 14, 14,
    1, 15, 8, 40,
    2, 20, 30, 30, 2, 80
]
 
# Read CDR data from a CSV file
# Assume the CSV file contains a single column named 'cdr' with fixed-length strings
cdr_file_path = 'ACC_JIO_CTAS_EAWB2202.20240703155048.9178.65070_BI.cdr'
cdr_df = spark.read.csv(cdr_file_path, header=True)
 
# Function to split fixed-length data
def split_fixed_length(line, lengths):
    positions = [sum(lengths[:i]) for i in range(len(lengths) + 1)]
    values = [line[positions[i]:positions[i+1]].strip() or 'NA' for i in range(len(lengths))]
    return values
 
# Split the CDR data
parsed_data = cdr_df.rdd.map(lambda row: split_fixed_length(row[0], lengths))
 
# Create a DataFrame
df = parsed_data.toDF(columns)
 
# Show the DataFrame
df.show(truncate=False)
 
# Write the DataFrame to a CSV file
output_path = 'parsed_cdr_data'
df.write.csv(output_path, header=True, mode='overwrite')
 
print(f"Data has been written to the '{output_path}' directory")
 
