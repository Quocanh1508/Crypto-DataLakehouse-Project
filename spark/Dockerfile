FROM eclipse-temurin:17-jdk-jammy

# Install Python 3 and basic utilities
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Map python3 to python
RUN ln -s /usr/bin/python3 /usr/bin/python

# Configure Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_WEBUI_PORT=8080
ENV SPARK_WORKER_WEBUI_PORT=8081

# Step 1: Download Spark 3.5.8 (Most stable for Delta 4.x and our pipeline)
RUN curl -sL \
  "https://dlcdn.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz" \
  -o /tmp/spark-3.5.8-bin-hadoop3.tgz

# Copy the Spark distribution and extract it
RUN tar -xzf /tmp/spark-3.5.8-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-3.5.8-bin-hadoop3 /opt/spark && \
    rm /tmp/spark-3.5.8-bin-hadoop3.tgz

# Add GCS Connector for Spark → gs:// support.
# Pinning to 2.2.22 (stable for Hadoop 3.3+ / Spark 4) to ensure proper ADC auth support
RUN curl -sL \
  "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar" \
  -o /opt/spark/jars/gcs-connector-hadoop3-2.2.22-shaded.jar

# Copy our custom startup script
COPY start-spark.sh /
RUN chmod +x /start-spark.sh && sed -i 's/\r$//' /start-spark.sh

# The script decides whether to start master or worker based on $SPARK_ROLE
ENTRYPOINT ["/start-spark.sh"]