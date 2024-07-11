from pyspark.sql import SparkSession
import os
from datetime import datetime
import csv

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CDR Processing") \
    .getOrCreate()

# HDFS input and output paths
input_path = "hdfs://jbdlha/Apps/warehouse/invotp/med/"
output_path = "hdfs://jbdlha/Apps/warehouse/invotp/processed/"

# Path to the processed files log to track already processed files
processed_files_log = "hdfs://jBDLH/Apps/warehouse/invotp/processed_files_log.txt"

# Function to read the log of processed files
def get_processed_files(log_path):
    if not os.path.exists(log_path):
        return set()
    with open(log_path, 'r') as f:
        return set(f.read().splitlines())

# Function to update the log of processed files
def update_processed_files(log_path, files):
    with open(log_path, 'a') as f:
        f.write('\n'.join(files) + '\n')

# Function to process CDR files
def process_cdr_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
        processed_data = []
        for line in lines:
            # Split fixed length fields to CSV format
            # Adjust offsets and lengths as per CDR file specification
            # Example (offsets and lengths are hypothetical):
            record = [
                line[0:21].strip(),
                line[21:41].strip(),
                line[41:49].strip(),
                line[49:57].strip(),
                line[57:67].strip(),
                line[67:95].strip(),
                line[95:123].strip(),
                line[123:143].strip(),
                line[143:163].strip(),
                line[163:177].strip(),
                line[177:191].strip(),
                line[191:192].strip(),
                line[192:207].strip(),
                line[207:215].strip(),
                line[215:255].strip(),
                line[255:257].strip(),
                line[257:277].strip(),
                line[277:307].strip(),
                line[307:337].strip(),
                line[337:339].strip(),
                # Add more fields as required
            ]
            processed_data.append(record)
    
    # Define CSV file path
    output_file = os.path.join(output_path, os.path.basename(file_path).replace('.cdr', '.csv'))
    
    # Write processed data to CSV
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(['Field1', 'Field2', 'Field3', 'Field4', 'Field5', 'Field6', 'Field7',
                        'Field8', 'Field9', 'Field10', 'Field11', 'Field12', 'Field13', 'Field14',
                        'Field15', 'Field16', 'Field17', 'Field18', 'Field19', 'Field20'])  # Adjust headers as needed
        # Write data
        writer.writerows(processed_data)

# Main function to run the job
def main():
    # Get list of already processed files
    processed_files = get_processed_files(processed_files_log)
    
    # Get list of new files to process
    files_to_process = [file for file in os.listdir(input_path) if file.endswith('.cdr') and file not in processed_files]
    
    # Process each new file
    for file in files_to_process:
        process_cdr_file(os.path.join(input_path, file))
    
    # Update the log with newly processed files
    update_processed_files(processed_files_log, files_to_process)

if __name__ == "__main__":
    main()
