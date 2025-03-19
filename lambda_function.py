import os
import json
import requests
from kafka import KafkaProducer
from boto3.session import Session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

KAFKA_BROKER_STRING = os.getenv('KAFKA_BROKER_STRING')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
DRIVEBC_API_URL = os.getenv('DRIVEBC_API_URL')
AWS_REGION = os.getenv('AWS_REGION')

session = Session()
credentials = session.get_credentials()


def sign_request(host, region):
    service = "kafka-cluster"
    request = AWSRequest(method='GET', url=f"https://{host}")
    SigV4Auth(credentials, service, region).add_auth(request)
    return request.headers['Authorization']


class IAMKafkaProducer:
    def __init__(self, broker, region):
        self.region = region
        self.broker = broker

        self.producer = KafkaProducer(
            bootstrap_servers=[self.broker],
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=self._sign_aws_request,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

    def _sign_aws_request(self):
        return sign_request(self.broker, self.region)

    def send(self, topic, key, message):
        future = self.producer.send(topic, key=key, value=message)
        result = future.get(timeout=10)
        return result


def fetch_drivebc_data():
    response = requests.get(DRIVEBC_API_URL)
    if response.status_code == 200:
        return response.json()
    else:
        print(
            f"Failed to fetch data from DriveBC API: {response.status_code} {response.text}")
        return None


def send_to_kafka(data):
    if not data:
        print("No data to send to Kafka")
        return
    producer = IAMKafkaProducer(KAFKA_BROKER_STRING, AWS_REGION)
    message = json.dumps(data)
    result = producer.send(KAFKA_TOPIC, key="drivebc", message=message)
    print("Message sent to Kafka: ", result)


def lambda_handler(event, context):
    data = fetch_drivebc_data()
    send_to_kafka(data)
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Data sent to Kafka'})
    }
