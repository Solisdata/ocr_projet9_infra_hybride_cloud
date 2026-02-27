# Les tickets sont produits en continu dans Redpanda
# objectif : PySpark permet de lire ces données en BATCH et de faire des calculs parallèles très rapidement
# spark_job_redpanda_batch.py

import os
from datetime import datetime, timedelta
import pandas as pd

# --- Configuration environnement Windows / Spark ---
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = r"C:\Users\mathi\Documents\8_OCR\Projet_9\ocr_projet9_infra_hybride_cloud\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\mathi\Documents\8_OCR\Projet_9\ocr_projet9_infra_hybride_cloud\venv\Scripts\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour,
    from_json, when, count, countDistinct, avg,
    sum as spark_sum, first, last,
    to_timestamp, lit
)
from pyspark.sql.types import StructType, StringType

BROKER = "127.0.0.1:19092"
TOPIC = "client_tickets"

def main():

    # --- Définition de la fenêtre temporelle : dernière heure ---
    now = datetime.now()
    last_hour = now - timedelta(hours=1)

    print("=== Spark Batch Job — Dernière heure ===")
    print(f"Fenêtre : {last_hour.strftime('%H:%M')} → {now.strftime('%H:%M')}")

    # --- Création de la session Spark ---
    spark = SparkSession.builder \
        .appName("KafkaTicketBatchStats_LastHour") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # --- Schéma du JSON reçu depuis Kafka ---
    schema = StructType() \
        .add("ticket_id", StringType()) \
        .add("client_id", StringType()) \
        .add("date_creation", StringType()) \
        .add("demande", StringType()) \
        .add("type_demande", StringType()) \
        .add("priorite", StringType())

    try:

        # --- Lecture complète du topic Kafka (mode batch) ---
        df_raw = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", BROKER) \
            .option("subscribe", TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        if df_raw.count() == 0:
            print("Aucun message dans le topic. Arrêt.")
            spark.stop()
            return

        # ======================================================
        # SAUVEGARDE BRUTE (ZONE BRONZE)
        # ======================================================

        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        raw_dir = f"./data_raw/batch_{timestamp}"
        os.makedirs(raw_dir, exist_ok=True)

        # Export via pandas pour éviter le bug FileOutputCommitter sur Windows
        df_raw.selectExpr(
            "CAST(value AS STRING) as json_str",
            "timestamp as kafka_ts"
        ).toPandas().to_parquet(f"{raw_dir}/raw.parquet", index=False)

        print(f"Backup RAW exporté → {raw_dir}/raw.parquet")

        # ======================================================
        # TRANSFORMATION DES DONNÉES
        # ======================================================

        df = (
            df_raw
            .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
            .select(from_json(col("json_str"), schema).alias("data"), col("kafka_ts"))
            .select("data.*", "kafka_ts")
            .withColumn("date_creation_ts", col("date_creation").cast("timestamp"))
            .filter(col("date_creation_ts").isNotNull())
            .filter(col("kafka_ts") >= lit(last_hour.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
            .withColumn("year",  year("date_creation_ts"))
            .withColumn("month", month("date_creation_ts"))
            .withColumn("day",   dayofmonth("date_creation_ts"))
            .withColumn("hour",  hour("date_creation_ts"))
            .withColumn("equipe_support",
                when(col("type_demande") == "support technique", "Equipe Tech")
                .when(col("type_demande") == "facturation", "Equipe Facturation")
                .otherwise("Equipe Generale")
            )
            .withColumn("priorite_level",
                when(col("priorite") == "basse", 1)
                .when(col("priorite") == "moyenne", 2)
                .when(col("priorite") == "haute", 3)
                .when(col("priorite") == "urgente", 4)
                .otherwise(0)
            )
        )

        total = df.count()
        print(f"✅ {total} tickets trouvés sur la dernière heure")

        if total == 0:
            print("Aucun ticket sur la dernière heure. Arrêt.")
            spark.stop()
            return

        # ======================================================
        # AGRÉGATIONS STATISTIQUES
        # ======================================================

        stats = (
            df.groupBy("type_demande", "equipe_support")
            .agg(
                count("ticket_id").alias("total_tickets"),
                spark_sum(when(col("priorite") == "basse", 1).otherwise(0)).alias("basse_count"),
                spark_sum(when(col("priorite") == "moyenne", 1).otherwise(0)).alias("moyenne_count"),
                spark_sum(when(col("priorite") == "haute", 1).otherwise(0)).alias("haute_count"),
                spark_sum(when(col("priorite") == "urgente", 1).otherwise(0)).alias("urgente_count"),
                avg("priorite_level").alias("priorite_moyenne"),
                countDistinct("client_id").alias("clients_uniques"),
                first("date_creation_ts").alias("premier_ticket"),
                last("date_creation_ts").alias("dernier_ticket")
            )
            .withColumn("basse_pct",   (col("basse_count")   / col("total_tickets") * 100))
            .withColumn("moyenne_pct", (col("moyenne_count") / col("total_tickets") * 100))
            .withColumn("haute_pct",   (col("haute_count")   / col("total_tickets") * 100))
            .withColumn("urgente_pct", (col("urgente_count") / col("total_tickets") * 100))
        )

        # ======================================================
        # EXPORT DES STATISTIQUES (ZONE SILVER)
        # ======================================================

        out_dir = f"./output_stats/batch_{timestamp}"
        os.makedirs(out_dir, exist_ok=True)

        # Export via pandas pour éviter le bug FileOutputCommitter sur Windows
        stats_pd = stats.toPandas()
        stats_pd.to_parquet(f"{out_dir}/stats.parquet", index=False)
        stats_pd.to_csv(f"{out_dir}/stats.csv", index=False, header=True)

        print(f"✅ Stats exportées → {out_dir}")

        print("\n=== Aperçu ===")
        stats.show(truncate=False)

    except Exception as e:
        print(f"Erreur : {e}")
        raise
    finally:
        spark.stop()
        print("Session Spark fermée")

if __name__ == "__main__":
    main()