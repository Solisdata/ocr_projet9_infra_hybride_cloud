#Les tickets sont produits en continu dans Redpanda
#objectif : PySpark permet de lire ces données en flux ou par lots et de faire des calculs parallèles très rapidement
# spark_job_redpanda.py
import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ["PATH"]
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import window

print("[1/5] Démarrage de la session Spark...")

spark = SparkSession.builder \
    .appName("ClientTickets") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .getOrCreate()       

spark.sparkContext.setLogLevel("ERROR")
print("[1/5] Session Spark démarrée avec succès")

print("[2/5] Définition du schéma...")
schema = StructType([
    StructField("ticket_id", StringType()),
    StructField("client_id", StringType()),
    StructField("date_creation", StringType()),
    StructField("demande", StringType()),
    StructField("type_demande", StringType()),
    StructField("priorite", StringType())
])
print("[2/5] Schéma défini")

BROKER = "127.0.0.1:19092"
TOPIC = "client_tickets"

print(f"[3/5] Connexion à Redpanda → broker: {BROKER}, topic: {TOPIC}")
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()
print("[3/5] Connexion Redpanda OK")

print("[4/5] Transformation des données...")
df_str = df_raw.selectExpr("CAST(value AS STRING) as json_str")
df_tickets = df_str.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
df_tickets = df_tickets.withColumn(
    "date_creation_ts",
    to_timestamp("date_creation", "yyyy-MM-dd HH:mm:ss"))
df_tickets = df_tickets.withColumn(
    "equipe_support",
    when(col("type_demande") == "support technique", "Equipe Tech")
    .when(col("type_demande") == "facturation", "Equipe Facturation")
    .otherwise("Equipe Generale")
)
df_count = df_tickets.groupBy("type_demande").agg(count("*").alias("nb_tickets"))
print("[4/5] Transformations définies")

print("[5/5] Lancement du stream... (en attente de messages Redpanda)")
#configuration traitement en parquet
query = df_tickets \
    .withWatermark("date_creation_ts", "1 minute") \
    .groupBy(
        window(col("date_creation_ts"), "10 minutes"),
        col("type_demande")
    ) \
    .agg(count("*").alias("nb_tickets")) \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./output_tickets") \
    .option("checkpointLocation", "./checkpoints") \
    .trigger(processingTime='10 seconds') \
    .start()
  #Checkpoints : Spark va mémoriser l’état du stream dans ./checkpoints. Si le job crash → au redémarrage il reprend à la dernière position Kafka.
  #format console avant : affichage dans terminal 
#   query = df_count.writeStream \
#     .outputMode("complete") \
#     .format("consolde") \
#     .option("path", "./output_tickets") \
#     .option("checkpointLocation", "./checkpoints") \
#     .trigger(processingTime='10 seconds') \
#     .start()

print(" Stream actif ! En écoute sur le topic:", TOPIC)
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stream interrompu par l'utilisateur")
except Exception as e:
    print("Erreur inattendue :", e)

