from pyspark.sql import SparkSession
import os

print("="*60)
print("Connecting to Spark to fetch Silver row count...")

spark = (
    SparkSession.builder
    .appName("CheckSilverCount")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")
    # GCS Auth
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
    .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", 
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/home/spark/.config/gcloud/application_default_credentials.json"))
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

bronze_path = "gs://crypto-lakehouse-group8/bronze"
silver_path = "gs://crypto-lakehouse-group8/silver"
gold_path   = "gs://crypto-lakehouse-group8/gold"

try:
    bronze_count = spark.read.format("delta").load(bronze_path).count()
    silver_count = spark.read.format("delta").load(silver_path).count()
    gold_count   = spark.read.format("delta").load(gold_path).count()
    print("="*60)
    print(f"🥉 BRONZE TABLE ROW COUNT: {bronze_count:,}")
    print(f"🥈 SILVER TABLE ROW COUNT: {silver_count:,}")
    print(f"🥇 GOLD TABLE ROW COUNT:   {gold_count:,}")
    print("="*60)
except Exception as e:
    print(f"Error reading tables: {e}")

spark.stop()
