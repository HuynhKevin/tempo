from pip._internal.operations import freeze

x = freeze.freeze()
for p in x:
    print(p)

import pulsar
from pyspark.sql import SparkSession
from influxdb import InfluxDBClient
import re

# class InfluxDBWriter:
#     def __init__(self):
#         self.client = InfluxDBClient(host='localhost', port=8086)

#     def open(self, partition_id, epoch_id):
#         print("Opened %d, %d" % (partition_id, epoch_id))
#         return True

#     def process(self, row):
#         influx_data = []            
#         influx_data.append("test_table, content={content}, id=5").format(content=row["value"])
#         self.client.write_points(influx_data, database="test", time_precision='ms', batch_size=10000, protocol='line')

#     def close(self, error):
#         self.client.__del__()
#         print("Closed with error: %s" % str(error))

def write(row):
    print("Row type ", type(row))

    client = InfluxDBClient(host='localhost', port=8086)
    influx_data = []
    print("Row value: ", row.value)
    contents = row.value.split(",")
    #re.escape(row["value"])
    influx_data.append("test_table2,content1=" + contents[0] + ",content2=" + contents[1].split("\n")[0] + " id=2 5")
    #influx_data.append("m1,location=location3,fruit=fruit2,id=id x=10,y=1,z=42i 1562458785618")
    client.write_points(influx_data, database="test", time_precision='ms', batch_size=10000, protocol='line')

class Consumer:
    def __init__(self) -> None:
        self.spark = SparkSession \
        .builder \
        .getOrCreate()
        #.master("local[*]") \
        #.getOrCreate()

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
                    # .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationToken") \
                    # .option("pulsar.client.authParams","token:" + TOKEN) \
                    # .option("pulsar.client.tlsTrustCertsFilePath","certs/ca.cert.pem") \
                    # .option("pulsar.client.tlsAllowInsecureConnection","false") \
                    # .option("pulsar.client.tlsHostnameVerificationenable","false") \
                    # .option("topic", PULSAR_TOPIC) \
                    # .load() \
                    # .selectExpr("CAST(value AS STRING)")

            #stream_df.writeStream.foreach(InfluxDBWriter()).start().awaitTermination()
            stream_df.writeStream.foreach(write).start().awaitTermination()

PULSAR_BROKER_ENDPOINT = "pulsar://127.0.0.1:6650"
PULSAR_ADMIN_ENDPOINT = "http://127.0.0.1:80"

# With TLS
# PULSAR_BROKER_ENDPOINT = "pulsar+ssl://127.0.0.1:6651"
# PULSAR_ADMIN_ENDPOINT = "https://127.0.0.1:443"

TOKEN="eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.eEiPXcZ7TDOn6eEmjWgnmYzOnjCJZyv3K2hNniz2KXt6S-ucnGdNcbKF-OahVJipbxwkQB7Msxq23XOMVr8uZkWaJLudY2GUsc7RfCsCCeTA7smE_fQRpxIQsto6hcEg0qst0n4-2jEbfLC-PHLpSRLARPpbmRkVbYCXgH4hqhj9LgTHtr1CpjTyYGXitfjmJKvxSamyfFZiaULqYLU6Bm4MWj7pNl6kgYwmbvz4xknrLDOV0lAgBlvAIJEEuTDz1nLIsKqj2VwHCMbbahtIRzeShdEVK_9PO7uSmiUWLHaIkdWq8jVgw5KcRLC4QzWPidbbjIQUQ-Mi2nl_TBv3Zg"
PULSAR_TOPIC = "apache/pulsar/test-topic"

consumer = Consumer()
consumer.collect_data(PULSAR_BROKER_ENDPOINT, PULSAR_ADMIN_ENDPOINT, PULSAR_TOPIC)
