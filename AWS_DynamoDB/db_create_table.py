import json
import boto3

# Read in API Credential File
with open('aws_credentials.json') as f:
    cred_data = json.load(f)

AWS_REGION = cred_data['main_region']
AWS_KEY = cred_data['access_key_id']
AWS_SECRET = cred_data['access_secret_key']

# Get the service resource.
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION,
                          aws_access_key_id=AWS_KEY,
                          aws_secret_access_key=AWS_SECRET)


# Create the DynamoDB table.
table = dynamodb.create_table(
    TableName='crypto_data_streaming',
    KeySchema=[
        {
            'AttributeName': 'name_coin',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'timestamp',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'name_coin',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'timestamp',
            'AttributeType': 'N'
        },
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)
