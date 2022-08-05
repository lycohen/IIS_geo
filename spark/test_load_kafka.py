from pyspark.shell import spark

TOPIC = 'cyxtera_onb_evt_geoclasificado'
BROKERS = 'dkafka02:9092'

options = {
    "kafka.sasl.mechanism": "PLAINTEXT",
    "kafka.bootstrap.servers": BROKERS,
    "subscribe": TOPIC,
}

df = spark \
    .readStream \
    .format("kafka") \
    .options(**options) \
    .load()

query = df.selectExpr("CAST(value AS STRING)").writeStream.format('json').outputMode("append").option("path", "test.json").option('checkpointLocation', '/galicia/d/landing_files/iis_geo').start().awaitTermination()