"""
Produces the Kafka messages to the Kafka topic in the config.json file
This file calls the coinrank api file to get the data
"""
import time
import json
from kafka import KafkaProducer
from Kafka_Files import coinrank_api

# Read in API Credential File
with open('credentials.json') as f:
    cred_data = json.load(f)

# Read in Config File
with open('config.json') as f:
    conf_data = json.load(f)

# Coinrank API Information
coin_api = cred_data['coinrank_api']['api_key']
coin_base_url = conf_data['Coinrank']['Url']

# Kafka Server Information
kafka_server = conf_data['Kafka']['Server']
kafka_topic = conf_data['Kafka']['Topic']
# Kakfa Delay Setting
delay_sec = int(conf_data['Project_Settings']['Kafka_Delay_Sec'])
# Kafka Producer
producer = KafkaProducer(bootstrap_servers=[kafka_server],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Coinrank UUID Coins
list_coins_data = ['Qwsogvtv82FCd', 'razxDUgYGNAdQ']  # Bitcoin, Ethereum

# Coinrank Get Request Headers
get_headers = {'x-access-token': coin_api}


# Iterate through list of coin data we want
def kafka_produce_message():
    for coin in list_coins_data:
        get_url = f"{coin_base_url}/coin/{coin}"
        # Call the API
        raw_json = coinrank_api.get_raw_data(get_url, get_headers)
        # Clean the Data
        filtered_json = coinrank_api.filter_response(raw_json)
        # Send the data to the Kafka Consumer
        producer.send(kafka_topic, filtered_json)
        producer.flush()
    return None


# File has to be ran directly
if __name__ == "__main__":
    while True:
        kafka_produce_message()
        print("Sent Kafka Message")
        time.sleep(delay_sec)
