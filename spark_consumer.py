from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import base64

# Prometheus pour exposition des métriques
from prometheus_client import start_http_server, Counter, Gauge, Summary
import threading
import time

#  Démarrage de la session Spark
print(" Démarrage du consommateur CDC Spark...")
spark = SparkSession.builder \
    .appName("DebeziumCDCConsumer-Prometheus") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#  Lire depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "cdc.public.transactions") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

#  Schéma pour le champ `amount` (DECIMAL PostgreSQL → Debezium)
amount_schema = StructType([
    StructField("scale", IntegerType(), True),
    StructField("value", StringType(), True)  # base64 encoded bytes
])

#  Schéma de la table `transactions`
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", LongType(), True),  # microsecond timestamp
    StructField("amount", amount_schema, True),
    StructField("currency", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("voucher_code", StringType(), True),
    StructField("affiliateid", StringType(), True),
])

#  Schéma Debezium enveloppe
debezium_schema = StructType([
    StructField("before", transaction_schema, True),
    StructField("after", transaction_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("source", StructType([
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("db", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
    ]), True)
])

#  Schéma racine du message Kafka
envelope_schema = StructType([
    StructField("schema", MapType(StringType(), StringType()), True),
    StructField("payload", debezium_schema, True)
])

#  Parser le JSON brut du message Kafka
parsed_df = df.select(
    from_json(col("value").cast("string"), envelope_schema).alias("data"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
)

# Extraire le payload Debezium
payload_df = parsed_df.select("data.payload.*", "topic", "partition", "offset", "kafka_timestamp")

#  Sélectionner et transformer les champs utiles
final_df = payload_df.select(
    col("op").alias("operation"),
    col("ts_ms").alias("event_timestamp"),
    col("source.table").alias("source_table"),
    col("after.transaction_id"),
    col("after.user_id"),
    (col("after.timestamp") / 1_000_000).cast("timestamp").alias("transaction_time"),
    col("after.amount"),
    col("after.currency"),
    col("after.city"),
    col("after.country"),
    col("after.merchant_name"),
    col("after.payment_method"),
    col("after.ip_address"),
    col("after.voucher_code"),
    col("after.affiliateid"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("kafka_timestamp")
).filter(col("after").isNotNull())

#  UDF pour décoder le DECIMAL Debezium
def decode_decimal(value_b64, scale):
    if value_b64 is None or scale is None:
        return None
    try:
        raw_bytes = base64.b64decode(value_b64)
        unscaled_value = int.from_bytes(raw_bytes, byteorder="big", signed=True)
        return float(unscaled_value / (10 ** scale))
    except Exception as e:
        print(f"[ERREUR UDF] Impossible de décoder value={value_b64}, scale={scale}, erreur={str(e)}")
        return None

decode_decimal_udf = udf(decode_decimal, DoubleType())

# Appliquer le décodage du montant
processed_df = final_df.withColumn(
    "amount_decimal",
    decode_decimal_udf(col("amount.value"), col("amount.scale"))
).withColumn(
    "change_type",
    when(col("operation") == "c", "INSERT")
    .when(col("operation") == "u", "UPDATE")
    .when(col("operation") == "d", "DELETE")
    .when(col("operation") == "r", "SNAPSHOT")
    .otherwise("UNKNOWN")
).withColumn(
    "processed_at", current_timestamp()
)

#  DÉMARRER LE SERVEUR PROMETHEUS SUR LE PORT 8081
print(" Démarrage du serveur Prometheus sur http://localhost:8081/metrics")
start_http_server(8081)

#  DÉFINIR LES MÉTRIQUES PROMETHEUS
transactions_counter = Counter('cdc_transactions_total', 'Nombre total de transactions', ['operation', 'country'])
amount_gauge = Gauge('cdc_current_amount_eur', 'Montant actuel en €', ['city', 'merchant'])
avg_amount_summary = Summary('cdc_transaction_amount_euro', 'Distribution des montants de transaction (en €)')
tps_gauge = Gauge('cdc_tps', 'Transactions par seconde (moyenne sur dernier batch)')

#  Fonction appelée à chaque batch Spark
def foreach_batch_update_metrics(batch_df, batch_id):
    print(f"📦 Batch {batch_id} - Traitement de {batch_df.count()} événements")

    if batch_df.isEmpty():
        return

    # Collecter les données utiles
    rows = batch_df.select(
        "change_type", "country", "city", "merchant_name", "amount_decimal"
    ).collect()

    nb_rows = len(rows)
    amount_values = []

    for row in rows:
        country = row.country or "unknown"
        city = row.city or "unknown"
        merchant = row.merchant_name or "unknown"

        #  Incrémenter compteur par opération et pays
        transactions_counter.labels(operation=row.change_type, country=country).inc()

        #  Mettre à jour le "dernier montant" par ville et marchand
        if row.amount_decimal is not None:
            amount_gauge.labels(city=city, merchant=merchant).set(row.amount_decimal)
            amount_values.append(row.amount_decimal)
            avg_amount_summary.observe(row.amount_decimal)

    #  Calculer TPS = nb_rows / période du trigger (10s)
    tps = nb_rows / 10.0
    tps_gauge.set(tps)

    print(f" Métriques mises à jour : {nb_rows} événements, TPS={tps:.2f}")

#  ÉCRIRE DANS PROMETHEUS VIA foreachBatch
query = processed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(foreach_batch_update_metrics) \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", "/tmp/spark-checkpoint-cdc-grafana") \
    .start()

print(" Spark en attente des messages CDC... Insérez des données dans PostgreSQL.")
query.awaitTermination()