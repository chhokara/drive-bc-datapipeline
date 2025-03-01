import json
import requests
import boto3
from datetime import datetime

S3_BUCKET = "drive-events-json"
S3_KEY_PREFIX = "road-incidents/"

API_URL = "https://api.open511.gov.bc.ca/events"

s3 = boto3.client('s3')


def lambda_handler(event, context):
    try:
        response = requests.get(API_URL)

        if response.status_code == 200:
            data = response.json()

            timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
            s3_key = f"{S3_KEY_PREFIX}incidents_{timestamp}.json"

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(data),
                ContentType='application/json'
            )

            return {
                'statusCode': 200,
                'body': json.dumps("Data successfully saved to {s3_key}")
            }

        else:
            return {
                'statusCode': response.status_code,
                'body': json.dumps("Failed to fetch data from API")
            }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
