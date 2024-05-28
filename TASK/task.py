import json
import time
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import SerializerError


schema_str = """
{
  "type": "record",
  "name": "Stock",
  "namespace": "com.example",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "timestamp", "type": "long"}
  ]
}
"""

schema = avro.loads(schema_str)


avro_producer_config = {
    'bootstrap.servers': 'localhost:19092',
    'on_delivery': lambda err, msg: print(f'Delivery report: {err}' if err else f'Message produced: {msg.value()}'),
    'schema.registry.url': 'http://localhost:18081'
}

producer = AvroProducer(avro_producer_config, default_value_schema=schema)


def send_stock_data(symbol, price, timestamp):
    stock_data = {
        "symbol": symbol,
        "price": price,
        "timestamp": timestamp
    }
    try:
        producer.produce(topic='stock_avro_topic', value=stock_data)
        producer.flush()
        print(f"Produced stock data: {stock_data}")
    except SerializerError as e:
        print(f"Message serialization failed: {e}")

# Example usage: publish some stock data
send_stock_data("AAPL", 150.0, int(time.time()))
send_stock_data("GOOGL", 2800.0, int(time.time()))