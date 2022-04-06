from pip._internal.operations import freeze

x = freeze.freeze()
for p in x:
    print(p)

import pulsar
from pyspark.sql import SparkSession
from influxdb import InfluxDBClient
from datetime import datetime

def write(row):
    print("Row type ", type(row))

    client = InfluxDBClient(host='influxdb.addons-dev-influxdb.svc.cluster.local', port=8086)
    print("Row value: ", row.value)
    contents = row.value.split(",")
    if contents[0] == "date":
        return
    date_time = datetime.strptime(contents[0], '%Y-%m-%d %H:%M:%S')
    json_body = [
            {
                "measurement": "test_table2",
                "tags": {
                    "station_meteo": 14578001,
                },
                "time": date_time.isoformat('T'),
                "fields": {
                    "dd": contents[1],
                    "hu": contents[2],
                    "precip": contents[3],
                    "hu": contents[4],
                    "td": contents[5],
                    "t": contents[6],
                    "psl": contents[7].split("\n")[0]
                }
            }
        ]

    client.write_points(json_body, database="test")

class Consumer:
    def __init__(self) -> None:
        self.spark = SparkSession \
        .builder \
        .master("local[*]") \
        .getOrCreate()

    def collect_data(self, PULSAR_BROKER_ENDPOINT, PULSAR_ADMIN_ENDPOINT, PULSAR_TOPIC):
        while True:
            stream_df = self.spark \
                    .readStream \
                    .format("pulsar") \
                    .option("service.url", PULSAR_BROKER_ENDPOINT) \
                    .option("admin.url", PULSAR_ADMIN_ENDPOINT) \
                    .option("topic", PULSAR_TOPIC) \
                    .load() \
                    .selectExpr("CAST(value AS STRING)")

            stream_df.writeStream.foreach(write).start().awaitTermination()

PULSAR_BROKER_ENDPOINT = "pulsar://pulsar-broker.addons-dev-pulsar.svc.cluster.local:6650"
PULSAR_ADMIN_ENDPOINT = "http://pulsar-broker.addons-dev-pulsar.svc.cluster.local:8080"

TOKEN="eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.eEiPXcZ7TDOn6eEmjWgnmYzOnjCJZyv3K2hNniz2KXt6S-ucnGdNcbKF-OahVJipbxwkQB7Msxq23XOMVr8uZkWaJLudY2GUsc7RfCsCCeTA7smE_fQRpxIQsto6hcEg0qst0n4-2jEbfLC-PHLpSRLARPpbmRkVbYCXgH4hqhj9LgTHtr1CpjTyYGXitfjmJKvxSamyfFZiaULqYLU6Bm4MWj7pNl6kgYwmbvz4xknrLDOV0lAgBlvAIJEEuTDz1nLIsKqj2VwHCMbbahtIRzeShdEVK_9PO7uSmiUWLHaIkdWq8jVgw5KcRLC4QzWPidbbjIQUQ-Mi2nl_TBv3Zg"
PULSAR_TOPIC = "apache/pulsar/test-topic"

consumer = Consumer()
consumer.collect_data(PULSAR_BROKER_ENDPOINT, PULSAR_ADMIN_ENDPOINT, PULSAR_TOPIC)
