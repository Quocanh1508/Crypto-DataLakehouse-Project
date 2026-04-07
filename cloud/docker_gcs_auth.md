# cloud/docker_gcs_auth.md
# ============================================================
# How to connect your local Docker containers to GCS
# WITHOUT distributing .json key files
# ============================================================

## Method: Application Default Credentials (ADC) Mount

This is the recommended, keyless method for local development.

### Step 1: Authenticate on your Windows host machine

```powershell
gcloud auth application-default login
```

This stores credentials in:
`C:\Users\QUOC ANH\AppData\Roaming\gcloud\application_default_credentials.json`

### Step 2: Mount credentials into Spark containers

Add this volume mount to `spark-master` and `spark-worker` in `docker-compose.yml`:

```yaml
  spark-master:
    volumes:
      - type: bind
        source: C:\Users\QUOC ANH\AppData\Roaming\gcloud
        target: /root/.config/gcloud
        read_only: true
    environment:
      - GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json
```

### Step 3: Add the GCS Connector JAR to Spark

The `spark/Dockerfile` needs the Hadoop GCS Connector:

```dockerfile
# Add GCS Connector for Spark → gs:// support
RUN curl -sL \
  "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar" \
  -o /opt/spark/jars/gcs-connector-hadoop3-latest.jar
```

### Step 4: SparkSession GCS config (in your PySpark jobs)

```python
spark = SparkSession.builder \
    .config("spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT") \
    .config("spark.delta.logStore.gs.impl",
            "io.delta.storage.GCSLogStore")   # Delta atomicity on GCS
    .getOrCreate()
```

---

## Handoff for Teammate (hive-site.xml)

```xml
<!-- GCS Warehouse -->
<property>
    <name>hive.metastore.warehouse.dir</name>
    <value>gs://crypto-lakehouse-group8/bronze/</value>
</property>

<!-- GCS FileSystem Implementation -->
<property>
    <name>fs.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
</property>
<property>
    <name>fs.AbstractFileSystem.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
</property>

<!-- GCP Project ID -->
<property>
    <name>fs.gs.project.id</name>
    <value>YOUR_PROJECT_ID</value>
</property>

<!-- Auth: Application Default Credentials (keyless) -->
<property>
    <name>google.cloud.auth.type</name>
    <value>APPLICATION_DEFAULT</value>
</property>
```
