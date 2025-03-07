from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests
import json
import time
import os

# Creating a SparkSession
# spark = SparkSession.builder.appName("ExchangeRate").getOrCreate()

# https://v6.exchangerate-api.com/v6/01dfe4b2d476f13ff9818893/latest/USD

# Making a request to web page to obtain a response called request and convert it to JSON
def make_request_currency(url: str="https://v6.exchangerate-api.com/v6/01dfe4b2d476f13ff9818893/latest/USD"):
    request = requests.get(url)
    CurrencyData = request.json()
    
    if request.status_code != 200:
        print("Error")
        return 
    
    return CurrencyData

def create_final_json_currency(json_data):
    currency = {
        "date": json_data['time_last_update_utc'],
        "base": json_data['base_code'],
        "USD": json_data['conversion_rates']['USD'], # mỹ
        "EUR": json_data['conversion_rates']['EUR'], # châu âu
        "GBP": json_data['conversion_rates']['GBP'], # anh
        "SEK": json_data['conversion_rates']['SEK'], # thụy điển
        "CHF": json_data['conversion_rates']['CHF'], # thụy sĩ
        "INR": json_data['conversion_rates']['INR'], # ấn độ
        "CNY": json_data['conversion_rates']['CNY'], # trung quốc
        "HKD": json_data['conversion_rates']['HKD'], # hồng kông
        "JPY": json_data['conversion_rates']['JPY'], # nhật bản
        "THB": json_data['conversion_rates']['THB'], # thái lan
        "VND": json_data['conversion_rates']['VND'] # việt nam
    }
    return currency

def save_to_file_currency(currency):
    timestr = time.strftime("%d-%m-%Y")
    dir = "/opt/airflow/data"
    if not os.path.exists(dir):
        os.makedirs(dir)
    file_path = os.path.join(dir, timestr + '-' + 'currency.json')
    try:
        with open(file_path, 'w') as file:
            json.dump(currency, file)
    except FileNotFoundError as ex:
        print(ex)

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=['kafka1:9092', 'kafka2:9093', 'kafka3:9094'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def send_to_kafka(producer, topic, data):
    try:
        producer.send(topic, value=data)
        producer.flush()
    except KafkaError as ex:
        print(ex)

def finalExecutionExchangeRate():
    data = make_request_currency()
    final_data = create_final_json_currency(data)
    save_to_file_currency(final_data)

    producer = create_producer()
    timestr = time.strftime("%d-%m-%Y")
    file_path = os.path.join("/opt/airflow/data", timestr + '-' + 'currency.json')
    with open(file_path, 'r') as file:
        value = json.load(file)
        send_to_kafka(producer, 'exchangerate', value)

if __name__ == "__main__":
    finalExecutionExchangeRate()