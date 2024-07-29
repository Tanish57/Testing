from pyspark.sql import SparkSession
from pyspark.sql.functions import col , when , coalesce , lit , substring , current_timestamp, date_format

# os.environ['PYSPARK_PYTHON'] = r"C:\Users\parth25.patel\AppData\Local\Programs\Python\Python311\python.exe"
# # os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = r"C:\Users\parth25.patel\AppData\Local\Programs\Python\Python311\python.exe"
# # C:\Users\parth25.patel\AppData\Local\Programs\Python\Python311\
# # C:\Users\parth25.patel\AppData\Local\Programs\Python\Python311\Lib\site-packages\pyspark

# # Initialize Spark session
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
# cdr_file_path = 'ACC_JIO.cdr'
cdr_file_path = 'ACC_JIO_CTAS_EAWB2202.20240703155048.9178.65070_BI.cdr'
cdr_df = spark.read.csv(cdr_file_path, header=True)

# Read table2 data from a CSV file
table2_file_path = 'Trunk_Group_202404100910.csv'
table2_df = spark.read.csv(table2_file_path, header=True)

# Read table3 data from a CSV file
table3_file_path = 'Network_Node_202404100910.csv'
table3_df = spark.read.csv(table3_file_path, header=True)

# Function to split fixed-length data
def split_fixed_length(line, lengths):
    positions = [sum(lengths[:i]) for i in range(len(lengths) + 1)]
    values = [line[positions[i]:positions[i+1]].strip() or 'NA' for i in range(len(lengths))]
    return values

# Split the CDR data
parsed_data = cdr_df.rdd.map(lambda row: split_fixed_length(row[0], lengths))
# Create a DataFrame for table1
table1_parsed_df = parsed_data.toDF(columns)
table1_parsed_df = table1_parsed_df.drop("discrete_rating_parameter_1", "data_unit" , "record_sequence_number","record_type","user_summarisation","user_data","user_data_2","link_field","user_data_3")
pandas_df = table1_parsed_df.toPandas()
table1_parsed_df.createOrReplaceTempView("table1")
table2_df.createOrReplaceTempView("table2")
table3_df.createOrReplaceTempView("table3")

# COLUMN:-  "INCOMING_POI"
incoming_sql_query = """
SELECT 
    table1.*, 
    table2.POI AS INCOMING_POI
FROM 
    table1
LEFT JOIN 
    table2
ON 
    table1.incoming_path = table2.ID AND table1.incoming_node = table2.FK_NNOD
"""
incoming_join_df = spark.sql(incoming_sql_query)

# COLUMN:-  "OUTGOING_POI"
incoming_join_df.createOrReplaceTempView("incoming_join")
outgoing_sql_query = """
SELECT 
    incoming_join.*, 
    table2.POI AS OUTGOING_POI
FROM 
    incoming_join
LEFT JOIN 
    table2
ON 
    incoming_join.outgoing_path = table2.ID AND incoming_join.outgoing_node = table2.FK_NNOD
"""
outgoing_join_df = spark.sql(outgoing_sql_query)

# INCOMING_OPERATOR
outgoing_join_df.createOrReplaceTempView("outgoing_join")

incoming_operator_sql_query = """
SELECT 
    outgoing_join.*, 
    table2.OPERATOR AS INCOMING_OPERATOR
FROM 
    outgoing_join
LEFT JOIN 
    table2
ON 
    outgoing_join.incoming_path = table2.ID AND outgoing_join.incoming_node = table2.FK_NNOD
"""
incoming_operator_join_df = spark.sql(incoming_operator_sql_query)

# OUTGOING_OPERATOR
incoming_operator_join_df.createOrReplaceTempView("incoming_operator_join")
outgoing_operator_sql_query = """
SELECT 
    incoming_operator_join.*, 
    table2.POI AS OUTGOING_OPERATOR
FROM 
    incoming_operator_join
LEFT JOIN 
    table2
ON 
    incoming_operator_join.outgoing_path = table2.ID AND incoming_operator_join.outgoing_node = table2.FK_NNOD
"""
outgoing_operator_join_df = spark.sql(outgoing_operator_sql_query)

# FRANCHISE
outgoing_operator_join_df.createOrReplaceTempView("outgoing_operator_join")
incoming_node_join_df = outgoing_operator_join_df.alias("outgoing_join").join(
    table3_df.alias("table3_incoming"),
    col("outgoing_join.incoming_node") == col("table3_incoming.ID"),
    how='left'
).select(
    col("outgoing_join.*"),
    col("table3_incoming.FK_ORGA_FRAN").alias("FRANCHISE_INCOMING")
)

# Join with table3 for outgoing_node
outgoing_node_join_df = incoming_node_join_df.alias("incoming_join").join(
    table3_df.alias("table3_outgoing"),
    col("incoming_join.outgoing_node") == col("table3_outgoing.ID"),
    how='left'
).select(
    col("incoming_join.*"),
    col("table3_outgoing.FK_ORGA_FRAN").alias("FRANCHISE_OUTGOING")
)

franchise_df = outgoing_node_join_df.withColumn('FRANCHISE', coalesce(outgoing_node_join_df['FRANCHISE_INCOMING'], outgoing_node_join_df['FRANCHISE_OUTGOING']))
# Drop the intermediate columns
franchise_df = franchise_df.drop("FRANCHISE_INCOMING", "FRANCHISE_OUTGOING")

# BILLING_OPERATOR
billing_operator_df = franchise_df.withColumn('BILLING_OPERATOR', coalesce(franchise_df['INCOMING_OPERATOR'], franchise_df['OUTGOING_OPERATOR']))

# BILLED_PRODUCT
billing_product_df = billing_operator_df.withColumn('BILLING_PRODUCT', 
                   when(billing_operator_df['INCOMING_PRODUCT'] != 'NA', billing_operator_df['INCOMING_PRODUCT'])
                   .otherwise(billing_operator_df['OUTGOING_PRODUCT']))

# CALL_COUNT
call_count_df = billing_product_df.withColumn('CALL_COUNT', lit(1))

# RATING_COMPONENT
rating_component_df = call_count_df.withColumn('RATING_COMPONENT', lit("TC"))

# TIER
tier_df = rating_component_df.withColumn('TIER', lit("INTRA"))

# CURRENCY
currency_df = tier_df.withColumn('CURRENCY', lit("INR"))

# CASH_FLOW
cash_flow_df = currency_df.withColumn('CASH_FLOW', 
                   when(currency_df['INCOMING_PATH'] != 'NA', lit("R"))
                   .otherwise(lit("E")))

# ACTUAL_USAGE
actual_usage_df = cash_flow_df.withColumn('ACTUAL_USAGE', 
                   (substring(col('event_duration'), 1, 4).cast('int') * 3600 +
                    substring(col('event_duration'), 5, 2).cast('int') * 60 +
                    substring(col('event_duration'), 7, 2).cast('int')))

# CHARGED_USAGE
charged_usage_df = actual_usage_df.withColumn('CHARGED_USAGE', 
                   (substring(col('event_duration'), 1, 4).cast('int') * 3600 +
                    substring(col('event_duration'), 5, 2).cast('int') * 60 +
                    substring(col('event_duration'), 7, 2).cast('int')))

# CHARGED_UNITS
charged_units_df = charged_usage_df.withColumn('CHARGED_UNITS', lit(0))

# UNIT_COST_USED
unit_cost_used_df = charged_units_df.withColumn('UNIT_COST_USED', lit(0))

# AMOUNT
amount_df = unit_cost_used_df.withColumn('AMOUNT', lit(0))

# COMPONENT_DIRECTION
component_direction_df = amount_df.withColumn('COMPONENT_DIRECTION', 
                   when(amount_df['INCOMING_PATH'] != 'NA', lit("I"))
                   .when(amount_df['OUTGOING_PATH'] != 'NA', lit("O"))
                   .when((amount_df['INCOMING_PATH'] != 'NA') & (amount_df['OUTGOING_PATH'] != 'NA'), lit("T"))
                   .otherwise(lit("E")))

# FLAT_RATE_CHARGE
flat_rate_df = component_direction_df.withColumn('FLAT_RATE_CHARGE', lit(0))

# PRODUCT_GROUP
product_group_df = flat_rate_df.withColumn('PRODUCT_GROUP', lit("TELE"))

# START_CALL_COUNT
start_call_count_df = product_group_df.withColumn('START_CALL_COUNT', lit(1))

# service_type
service_type_df = start_call_count_df.withColumn('service_type', lit("Access-Voice"))

# switch_id
switch_id_df = service_type_df.withColumn('switch_id', 
                   when(service_type_df['INCOMING_NODE'] != 'NA', service_type_df['INCOMING_NODE'])
                   .otherwise(service_type_df['OUTGOING_NODE']))

# trunk_group_id
trunk_group_id_df = switch_id_df.withColumn('trunk_group_id', 
                   when(switch_id_df['INCOMING_PATH'] != 'NA', switch_id_df['INCOMING_PATH'])
                   .otherwise(switch_id_df['OUTGOING_PATH']))

# poi_id
poi_id_df = trunk_group_id_df.withColumn('poi_id', coalesce(trunk_group_id_df['INCOMING_POI'], trunk_group_id_df['OUTGOING_POI']))

# product_id
product_id_df = poi_id_df.withColumn('product_id', 
                   when(poi_id_df['INCOMING_PRODUCT'] != 'NA',poi_id_df['INCOMING_PRODUCT'])
                   .otherwise(poi_id_df['OUTGOING_PRODUCT']))

# network_operator_id
network_operator_id_df = product_id_df.withColumn('network_operator_id', coalesce(product_id_df['INCOMING_OPERATOR'], product_id_df['OUTGOING_OPERATOR']))

# INPUTFILENAME
file_name = cdr_file_path.split("/")[-1]
input_file_df = network_operator_id_df.withColumn('INPUTFILENAME', lit(file_name))

# PROCESSEDTIME
processed_time_df = input_file_df.withColumn("current_date_time", date_format(current_timestamp(), "dd/MM/yyyy HH:mm"))

# SOURCETYPE
source_type_df = processed_time_df.withColumn('SOURCETYPE', lit("MED"))





resullt_df = source_type_df.toPandas()
output_csv_path = 'results.csv'
resullt_df.to_csv(output_csv_path, header=True)

# print(f"Data has been successfully joined and saved to {output_csv_path}")
spark.stop()
















# .cdr

# incoming_node	outgoing_node	event_start_date	event_start_time	event_duration	anum	bnum	incoming_path	outgoing_path	incoming_product	outgoing_product	event_direction	discrete_rating_parameter_1	data_unit	record_sequence_number	record_type	user_summarisation	user_data	user_data_2	link_field	user_data_3
# EACTACCKO	NA	20240703	15423989	5400	9.17865E+11	9.13105E+15	RJILACC-WB	NA	MMHT	NA	NA	NA	NA	400665820	NA	NA	NA	405840	0	63380712e34041e507eba6bfdf81f7f4


# TRUNK

# ID|NAME|FK_NNOD|OPERATOR|POI
# BJPUR1RCI1N|BSNL Jaipur L1 Tax|NO01ACCRJ|BSNLRJ|BSJAIPL1
# BAGRA1SCD1N|BSNL Agra Int 1|NO01ACCUW|BSNLUW|BSAGRAI1
# CJMMU1JCE1N|Cellone Jammu Local|NO01ACCJK|CEL1JK|BSJAMMC1
# IDLHI1DAD1N|IDEA Delhi|NO01ACCDL|IDEAIL|ILIDEADL
# IMRUT1SAD1N|IDEA Uttar-Pradesh(West)|NO01ACCUW|IDEAIL|ILIDEAUW
# IDLHI1DAF1N|IDEA Delhi NLD|NO01ACCDL|IDEANL|NLIDEADL


# Network


# ID|NAME|FK_ORGA_FRAN
# EAA1ACCAS|RJIL Access(ALU) Assam|RJILAS
# EAA1ACCBH|RJIL Access(ALU) Bihar|RJILBH
# EAA1ACCKO|RJIL Access(ALU) Kolkata|RJILKO
# EAA1ACCNE|RJIL Access(ALU) North East|RJILNE
# EAA1ACCOR|RJIL Access(ALU) Orissa|RJILOR



# def convert_txt_to_csv(input_file_path, output_file_path):
#     # Read the pipe-separated text file into a DataFrame
#     df = pd.read_csv(input_file_path, sep='|')
    
#     # Save the DataFrame to a CSV file
#     df.to_csv(output_file_path, index=False)

# # Define the input and output file paths
# input_file_path = 'Network_Node_202404100910.csv'  # Replace with the path to your input text file
# output_file_path = 'Network_Node_202404100910.csv'  # Replace with the desired path for the output CSV file

# # Call the function to convert the file
# convert_txt_to_csv(input_file_path, output_file_path)



