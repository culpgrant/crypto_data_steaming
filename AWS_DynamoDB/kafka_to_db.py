import ast
import json
from decimal import Decimal
import boto3
from kafka import KafkaConsumer

# Read in API Credential File
with open('aws_credentials.json') as f:
    cred_data = json.load(f)

KAFKA_TOPIC = 'coinrank'
DECIMAL_COLUMNS = ['volume', 'market_cap', 'total_supply', 'price', 'percent_change_24hr']
CONSUMER = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                         enable_auto_commit=True, group_id='my-group', auto_commit_interval_ms=1000)
AWS_REGION = cred_data['main_region']
AWS_KEY = cred_data['access_key_id']
AWS_SECRET = cred_data['access_secret_key']
# Get the service resource.
DYNAMODB = boto3.resource('dynamodb', region_name=AWS_REGION,
                          aws_access_key_id=AWS_KEY,
                          aws_secret_access_key=AWS_SECRET)
TABLE = DYNAMODB.Table('crypto_data_streaming')


def decode_message(mes):
    mes = mes.decode('utf-8')
    return mes


def transform_message(item):
    for key, value in item.items():
        if key in DECIMAL_COLUMNS:
            item[key] = round((Decimal(value)), 2)
    return item


def send_to_dynamo(item):
    try:
        response = TABLE.put_item(Item=item)
        return response
    except Exception as e:
        print(f"Here is the exception: {e}")
        # Log this here


for message in CONSUMER:
    byte_str = message.value
    # Decode the Message
    message_str = decode_message(byte_str)
    # Convert the string to a dict
    message_dict = ast.literal_eval(message_str)
    # Transform the Dict for AWS Dynamodb
    message_clean = transform_message(message_dict)
    # Send the data to Dynamo DB
    dynamo_response = send_to_dynamo(message_clean)
    print(dynamo_response)
