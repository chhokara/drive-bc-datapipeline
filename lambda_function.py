import json
import requests
import boto3
from datetime import datetime
from confluent_kafka import Producer
import os


def get_bootstrap_servers():
    client = boto3.client('kafka')
    response = client.get_bootstrap_brokers(
        ClusterArn=os.environ['MSK_CLUSTER_ARN'])
    return response["BootstrapBrokerStringTls"]


KAFKA_BROKER = get_bootstrap_servers()

producer = Producer({
    'bootstrap.servers': KAFKA_BROKER,
    'security.protocol': 'SSL',
    'ssl.ca.location': '/opt/AmazonRootCA1.pem'
})

s3 = boto3.client('s3')


def send_to_kafka(events):
    try:
        for event in events[:50]:
            event_id = event.get("id", "unknown_id")
            producer.produce(os.environ['KAFKA_TOPIC'], key=event_id,
                             value=json.dumps(event))
        producer.flush()
        print(f"Successfully sent {len(events[:50])} events to Kafka.")
    except Exception as e:
        print(f"Failed to send events to Kafka: {str(e)}")


def lambda_handler(event, context):
    try:
        response = requests.get(os.environ['API_URL'])

        if response.status_code == 200:
            data = response.json()
            events = data.get("events", [])

            timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
            s3_key = f"{os.environ['S3_KEY_PREFIX']}incidents_{timestamp}.json"

            s3.put_object(Bucket=os.environ['S3_BUCKET'], Key=s3_key, Body=json.dumps(
                data), ContentType='application/json')

            print(f"Data successfully saved to S3: {s3_key}")

            if events:
                send_to_kafka(events)

            return {'statusCode': 200, 'body': json.dumps(f"Data saved to S3 and {len(events[:50])} events sent to Kafka.")}

        else:
            return {'statusCode': response.status_code, 'body': json.dumps("Failed to fetch data from API")}

    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f"Error: {str(e)}")}
