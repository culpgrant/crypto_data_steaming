import ast
import json
import logging
import time
from decimal import Decimal
import boto3
import botocore
from kafka import KafkaConsumer


KAFKA_TOPIC = 'coinrank'
DECIMAL_COLUMNS = ['volume', 'market_cap', 'total_supply', 'price', 'percent_change_24hr']
DYN_DB_TABLE_NAME = 'crypto_data_streaming'


def aws_dynamo_connect(reg, key, secret):
    db_connection = boto3.resource('dynamodb', region_name=reg, aws_access_key_id=key,
                                   aws_secret_access_key=secret)
    return db_connection


def decode_message(mes):
    mes = mes.decode('utf-8')
    return mes


def transform_message(item):
    for key, value in item.items():
        if key in DECIMAL_COLUMNS:
            item[key] = round((Decimal(value)), 2)
    return item


def send_to_dynamo(item, db_table):
    try:
        response = db_table.put_item(Item=item)
        return response
    except botocore.exceptions.ClientError as aws_error:
        if aws_error.response['Error']['Code'] == 'LimitExceededException':
            logging.warning('API call limit exceeded; backing off and retrying...')
            time.sleep(1)
        else:
            logging.error(aws_error)
            raise aws_error


if __name__ == "__main__":
    # Logging Configuration
    logging.basicConfig(level=logging.WARNING,
                        filename=r"/Users/GrantCulp/PycharmProjects/Crypto_Data_Streaming/Log_Files/log_file.log",
                        format="%(asctime)s - %(filename)s - %(message)s")
    # Setup Kafka Consumer
    CONSUMER = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='my-group', auto_commit_interval_ms=1000)
    # Read in API Credential File
    with open('aws_credentials.json') as f:
        cred_data = json.load(f)
    # Connect to AWS Dynamo DB
    aws_region = cred_data['main_region']
    aws_key = cred_data['access_key_id']
    aws_secret = cred_data['access_secret_key']
    dynamo_db = aws_dynamo_connect(aws_region, aws_key, aws_secret)
    # Specific Table in Dynamo DB
    table = dynamo_db.Table(DYN_DB_TABLE_NAME)

    for message in CONSUMER:
        byte_str = message.value
        # Decode the Message
        message_str = decode_message(byte_str)
        # Convert the string to a dict
        message_dict = ast.literal_eval(message_str)
        # Transform the Dict so AWS Dynamodb accepts it (Decimal manipulation)
        message_clean = transform_message(message_dict)
        # Send the data to Dynamo DB
        dynamo_response = send_to_dynamo(message_clean, table)
