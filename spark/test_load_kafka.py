# pyspark --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2
from pyspark.shell import spark

TOPIC = 'cyxtera_onb_evt_geoclasificado'
BROKERS = 'dkafka02:9092'

options = {
    "kafka.sasl.mechanism": "PLAINTEXT",
    "kafka.bootstrap.servers": BROKERS,
    "subscribe": TOPIC,
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
    "minOffsetPerTrigger": "50000",
    "maxTriggerDelay": "300"
}

df = spark \
    .readStream \
    .format("kafka") \
    .options(**options) \
    .load()

query = df.selectExpr("CAST(value AS STRING)")\
    .writeStream\
    .option("path", "/galicia/d/landing_files/iis_geo/test")\
    .option('checkpointLocation', '/galicia/d/landing_files/iis_geo/checks') \
    .start()\
    .awaitTermination()

