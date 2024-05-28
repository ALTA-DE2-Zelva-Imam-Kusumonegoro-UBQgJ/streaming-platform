import time
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

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
    'bootstrap.servers': 'localhost:18080',
    'schema.registry.url': 'http://localhost:18081'
}

producer = AvroProducer(avro_producer_config, default_value_schema=schema)

def send_stock_data(symbol, price):
    timestamp = int(time.time())
    stock_data = {"symbol": symbol, "price": price, "timestamp": timestamp}
    producer.produce(topic='stock_avro_topic', value=stock_data)
    producer.flush()
    print(f"Produced stock data: {stock_data}")

send_stock_data("AAPL", 150.0)
send_stock_data("GOOGL", 2800.0)
