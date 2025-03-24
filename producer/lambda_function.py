import boto3
import json
import requests
import os


kinesis = boto3.client('kinesis')

DRIVEBC_API_URL = os.environ.get("DRIVEBC_API_URL")
KINESIS_STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME")


def fetch_drivebc_data():
    response = requests.get(DRIVEBC_API_URL)
    if response.status_code == 200:
        print("Data fetched from DriveBC API")
        return response.json()
    else:
        print(
            f"Failed to fetch data from DriveBC API: {response.status_code} {response.text}")
        return None


def send_to_kinesis(data):
    if not data:
        print("No data to send to Kinesis")
        return

    response = kinesis.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=json.dumps(data),
        PartitionKey='lambda-producer'
    )

    print(f"Data sent to Kinesis: {response}")


def lambda_handler(event, context):
    data = fetch_drivebc_data()
    send_to_kinesis(data)
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Data sent to Kinesis'})
    }
