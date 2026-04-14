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

silver_path = "gs://crypto-lakehouse-group8/silver"
try:
    df = spark.read.format("delta").load(silver_path)
    count = df.count()
    print("="*60)
    print(f"✅ CURRENT SILVER TABLE ROW COUNT: {count:,}")
    print("="*60)
except Exception as e:
    print(f"Error reading silver: {e}")

spark.stop()
