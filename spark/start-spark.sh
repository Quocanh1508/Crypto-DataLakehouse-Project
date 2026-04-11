#!/bin/bash

# start-spark.sh
# A script to start either a Spark Master or Spark Worker.

JMX_AGENT="-javaagent:/opt/spark/jars/jmx_prometheus_javaagent-0.20.0.jar=9108:/opt/spark/conf/jmx-prometheus.yaml"
if [ -n "${SPARK_MASTER_OPTS:-}" ]; then
  export SPARK_MASTER_OPTS="${SPARK_MASTER_OPTS} ${JMX_AGENT}"
else
  export SPARK_MASTER_OPTS="${JMX_AGENT}"
fi
if [ -n "${SPARK_WORKER_OPTS:-}" ]; then
  export SPARK_WORKER_OPTS="${SPARK_WORKER_OPTS} ${JMX_AGENT}"
else
  export SPARK_WORKER_OPTS="${JMX_AGENT}"
fi

if [ "$SPARK_ROLE" == "master" ]; then
    echo "Starting Spark Master..."
    export SPARK_MASTER_HOST=0.0.0.0
    /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
elif [ "$SPARK_ROLE" == "worker" ]; then
    echo "Starting Spark Worker connecting to $SPARK_MASTER_URL ..."
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER_URL
else
    echo "SPARK_ROLE must be 'master' or 'worker'"
    exit 1
fi
