import json
import base64


def lambda_handler(event, context):
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        data = json.loads(payload)

        print("Received data from Kinesis:")
        print(json.dumps(data, indent=2))

    return {
        "statusCode": 200,
        "body": json.dumps('Successfully processed records')
    }
