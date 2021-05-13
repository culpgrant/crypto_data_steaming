"""
Python file to send the request to the CoinRank API
Four functions call get_raw_data to get all the raw data
Then call filter_response to get a filtered response from the raw data output
"""
import requests
import time


def current_unix_time():
    return int(round(time.time()))


def get_raw_data(url, api_headers):
    response = requests.get(url, headers=api_headers)
    status_code = response.status_code
    if status_code == 200:
        return response.json()['data']['coin']
    else:
        print(status_code)
        return {}


def parse_coin_data(coin_data):
    try:
        return {"name_coin": coin_data["name"],
                "symbol_coin": coin_data["symbol"],
                "uuid": coin_data["uuid"],
                "number_of_markets": coin_data["numberOfMarkets"],
                "volume": coin_data["24hVolume"],
                "market_cap": coin_data["marketCap"],
                "total_supply": coin_data['supply']["total"],
                "price": coin_data["price"],
                "percent_change_24hr": coin_data["change"],
                "timestamp": current_unix_time()}
    except KeyError as e:
        print(e)
        return {}


def filter_response(raw_json):
    filtered_json = parse_coin_data(raw_json)
    return filtered_json
