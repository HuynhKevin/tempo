import pulsar
from pyspark.sql import SparkSession

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
                    # .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationToken") \
                    # .option("pulsar.client.authParams","token:" + TOKEN) \
                    # .option("pulsar.client.tlsTrustCertsFilePath","certs/ca.cert.pem") \
                    # .option("pulsar.client.tlsAllowInsecureConnection","false") \
                    # .option("pulsar.client.tlsHostnameVerificationenable","false") \
                    # .option("topic", PULSAR_TOPIC) \
                    # .load() \
                    # .selectExpr("CAST(value AS STRING)")
        
            stream_df.writeStream.format("console").start().awaitTermination()
                
# With TLS
# PULSAR_BROKER_ENDPOINT = "pulsar+ssl://127.0.0.1:6651"
# PULSAR_ADMIN_ENDPOINT = "https://127.0.0.1:443"

#PULSAR_BROKER_ENDPOINT = "pulsar+ssl://stream.dev.client.graal.systems:6651"
#PULSAR_ADMIN_ENDPOINT = "https://stream.dev.client.graal.systems:443"

# Without TLS
# PULSAR_BROKER_ENDPOINT = "pulsar://host.docker.internal:6650"
# PULSAR_ADMIN_ENDPOINT = "http://host.docker.internal:80"
PULSAR_BROKER_ENDPOINT = "pulsar://pulsar-broker.addons-dev-pulsar.svc.cluster.local:6650"
PULSAR_ADMIN_ENDPOINT = "http://pulsar-broker.addons-dev-pulsar.svc.cluster.local:8080"

TOKEN = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.eEiPXcZ7TDOn6eEmjWgnmYzOnjCJZyv3K2hNniz2KXt6S-ucnGdNcbKF-OahVJipbxwkQB7Msxq23XOMVr8uZkWaJLudY2GUsc7RfCsCCeTA7smE_fQRpxIQsto6hcEg0qst0n4-2jEbfLC-PHLpSRLARPpbmRkVbYCXgH4hqhj9LgTHtr1CpjTyYGXitfjmJKvxSamyfFZiaULqYLU6Bm4MWj7pNl6kgYwmbvz4xknrLDOV0lAgBlvAIJEEuTDz1nLIsKqj2VwHCMbbahtIRzeShdEVK_9PO7uSmiUWLHaIkdWq8jVgw5KcRLC4QzWPidbbjIQUQ-Mi2nl_TBv3Zg"
PULSAR_TOPIC = "apache/pulsar/test-topic"

consumer = Consumer()
consumer.collect_data(PULSAR_BROKER_ENDPOINT, PULSAR_ADMIN_ENDPOINT, PULSAR_TOPIC)
