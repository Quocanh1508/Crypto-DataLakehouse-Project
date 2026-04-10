# Extend Starburst HMS with Hadoop 3 GCS connector (matches Spark gcs-connector 2.2.22).
FROM starburstdata/hive:3.1.2-e.18

USER root
RUN curl -fsSL \
  "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar" \
  -o /opt/apache-hive-3.1.2-bin/lib/gcs-connector-hadoop3-2.2.22-shaded.jar

USER hive
