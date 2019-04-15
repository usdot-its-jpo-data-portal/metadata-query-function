import boto3
import dateutil
import glob
import json
import logging
import os
import queue
import time
from queries import MetadataQueries

USE_LOCAL_DATA = True # whether to load data from S3 (false) or locally (true)
LOCAL_DATA_REPOSITORY = "s3data/usdot-its-cvpilot-public-data" # path to local directory containing s3 data

### Query to run
METADATA_QUERY = MetadataQueries.query3_goodOtherRecordCount

### Data source configuration settings
PREFIX_STRINGS = ["wydot/BSM/2018/12", "wydot/BSM/2019/01", "wydot/BSM/2019/02", "wydot/BSM/2019/03", "wydot/BSM/2019/04"]
S3_BUCKET = "usdot-its-cvpilot-public-data"
DATA_PROVIDERS = ["wydot"]
MESSAGE_TYPES = ["BSM"]

def lambda_handler(event, context):

    if USE_LOCAL_DATA:
        print("NOTE: Using local data in directory '%s'" % LOCAL_DATA_REPOSITORY)

    # Create a list of analyzable S3 files
    s3_client = boto3.client('s3')
    s3_file_list = []
    for prefix in PREFIX_STRINGS:
        matched_file_list = list_s3_files_matching_prefix(s3_client, prefix)
        print("Queried for S3 files matching prefix string '%s'. Found %d matching files." % (prefix, len(matched_file_list)))
        print("Matching files: [%s]" % ", ".join(matched_file_list))
        s3_file_list.extend(matched_file_list)

    perform_query(s3_client, s3_file_list, METADATA_QUERY)
    return

def perform_query(s3_client, s3_file_list, query_function):
    total_records = 0
    total_records_in_timeframe = 0
    total_records_not_in_timeframe = 0
    file_num = 1
    query_start_time = time.time()

    for filename in s3_file_list:
        file_process_start_time = time.time()
        print("============================================================================")
        print("Analyzing file (%d/%d) '%s'" % (file_num, len(s3_file_list), filename))
        print("Query being performed: %s" % str(METADATA_QUERY))
        file_num += 1
        record_list = extract_records_from_file(s3_client, filename)
        records_in_timeframe = 0
        records_not_in_timeframe = 0
        for record in record_list:
            total_records += 1
            if query_function(record):
                records_in_timeframe += 1
            else:
                records_not_in_timeframe += 1
        print("Records satisfying query constraints found in this file: \t%d" % records_in_timeframe)
        print("Total records found satisfying query constraints so far: \t\t%d" % total_records_in_timeframe)
        print("Records NOT found satisfying query constraints: \t\t\t\t%d" % records_not_in_timeframe)
        print("Total records NOT found satisfying query constraints so far: \t\t\t%d" % total_records_not_in_timeframe)
        time_now = time.time()
        print("Time taken to process this file: \t\t\t%.3f" % (time_now - file_process_start_time))
        time_elapsed = (time_now - query_start_time)
        avg_time_per_file = time_elapsed/file_num
        avg_time_per_record = time_elapsed/total_records
        est_time_remaining = avg_time_per_file * (len(s3_file_list) - file_num)
        print("Time elapsed so far: \t\t\t\t\t%.3f" % time_elapsed)
        print("Average time per file: \t\t\t\t\t%.3f" % avg_time_per_file)
        print("Average time per record: \t\t\t\t%.6f" % avg_time_per_record)
        print("Estimated time remaining: \t\t\t\t%.3f" % est_time_remaining)
        total_records_in_timeframe += records_in_timeframe
        total_records_not_in_timeframe += records_not_in_timeframe
    print("Total number of records found satisfying query constraints: %d (Total number of records not found satisfying query constraints: %d" % (total_records_in_timeframe, total_records_not_in_timeframe))

### Returns a list of records from a given file
def extract_records_from_file(s3_client, filename):
    if USE_LOCAL_DATA:
        with open(filename, 'r') as f:
            return f.readlines()
    else:
        s3_file = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key=filename,
        )
        return list(s3_file['Body'].iter_lines()) ### iter_lines() is significantly faster than read().splitlines()

### Returns filenames from an S3 list files (list_objects) query
def list_s3_files_matching_prefix(s3_client, prefix_string):
    if USE_LOCAL_DATA:
        try:
            files_and_directories = glob.glob(LOCAL_DATA_REPOSITORY+"/"+prefix_string+"/**/*", recursive=True)
            files_only = []
            for filepath in files_and_directories:
                if os.path.isfile(filepath):
                    files_only.append(filepath)
            return files_only
        except FileNotFoundError as e:
            return []
    else:
        response = list_s3_objects(s3_client, prefix_string)
        filenames = []
        if response.get('Contents'):
            [filenames.append(item['Key']) for item in response.get('Contents')]
        while response.get('NextContinuationToken'):
            response = list_s3_objects(s3_client, prefix_string, response.get('NextContinuationToken'))
            if response.get('Contents'):
                [filenames.append(item['Key']) for item in response.get('Contents')]
        return filenames

def list_s3_objects(s3_client, prefix_string, continuation_token=None):
    if continuation_token:
        return s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix_string,
            ContinuationToken=continuation_token,
        )
    else:
        return s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix_string,
        )

if __name__ == "__main__":
    lambda_handler(None, None)
