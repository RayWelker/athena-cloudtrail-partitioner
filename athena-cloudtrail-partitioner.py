import os
import sys
import csv
import boto3
import botocore
import time
from datetime import date, timedelta
from retrying import retry

# configuration
s3_bucket = 'my-test-bucket'                        # S3 Bucket name
s3_output  = 's3://'+ s3_bucket +'/query-results/'  # S3 Bucket to store results
database  = 'default'                               # The database to which the query belongs
cloudtrail_log_bucket = 'my-cloudtrail-log-bucket'  # The S3 Bucket name where cloudtrail logs are stored
account_id = '12345678901'                          # The AWS account ID

# init clients
athena = boto3.client('athena')
s3     = boto3.resource('s3')

@retry(stop_max_attempt_number = 10,
    wait_exponential_multiplier = 300,
    wait_exponential_max = 1 * 60 * 1000)
def poll_status(_id):
    result = athena.get_query_execution( QueryExecutionId = _id )
    state  = result['QueryExecution']['Status']['State']

    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        raise Exception

def run_query(query, database, s3_output):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
    })

    QueryExecutionId = response['QueryExecutionId']
    result = poll_status(QueryExecutionId)

    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        print("Query SUCCEEDED: {}".format(QueryExecutionId))

        return 'SUCCEEDED'

def daterange(start_date, end_date):
  for n in range(int((end_date - start_date).days)):
    yield start_date + timedelta(n)

if __name__ == '__main__':
  start_date = date(2021, 8, 1)
  end_date = date(2022, 1, 27)
  for single_date in daterange(start_date, end_date):
    year = single_date.strftime("%Y")
    month = single_date.strftime("%m")
    day = single_date.strftime("%d")

    # SQL Query to execute
    query = (f"""
        ALTER TABLE cloudtrail_logs_partitioned ADD IF NOT EXISTS
          PARTITION (
              region = 'us-east-1',
              year = {int(year)},
              month = {int(month)},
              day = {int(day)}
            )
        LOCATION 's3://{cloudtrail_log_bucket}/AWSLogs/{account_id}/CloudTrail/us-east-1/{year}/{month}/{day}/';
    """)

    print("Executing query: {}".format(query))
    result = run_query(query, database, s3_output)

    print(f"Results: {result}")