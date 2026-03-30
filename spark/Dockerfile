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

# Copy the Spark distribution and extract it
COPY spark-4.1.1-bin-hadoop3.tgz /tmp/
RUN tar -xzf /tmp/spark-4.1.1-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-4.1.1-bin-hadoop3 /opt/spark && \
    rm /tmp/spark-4.1.1-bin-hadoop3.tgz

# Copy our custom startup script
COPY start-spark.sh /
RUN chmod +x /start-spark.sh

# The script decides whether to start master or worker based on $SPARK_ROLE
ENTRYPOINT ["/start-spark.sh"]
