#!/bin/bash

# start-spark.sh
# A script to start either a Spark Master or Spark Worker.

if [ "$SPARK_ROLE" == "master" ]; then
    echo "Starting Spark Master..."
    export SPARK_MASTER_HOST=0.0.0.0
    export SPARK_MASTER_OPTS="-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=9108:/opt/jmx/jmx-config.yaml"
    # Docker: start-master.sh dùng spark-daemon (nohup) — mặc định container thoát ngay
    export SPARK_NO_DAEMONIZE=1
    exec /opt/spark/sbin/start-master.sh
elif [ "$SPARK_ROLE" == "worker" ]; then
    echo "Starting Spark Worker connecting to $SPARK_MASTER_URL ..."
    export SPARK_WORKER_OPTS="-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=9109:/opt/jmx/jmx-config.yaml"
    export SPARK_NO_DAEMONIZE=1
    exec /opt/spark/sbin/start-worker.sh "$SPARK_MASTER_URL"
else
    echo "SPARK_ROLE must be 'master' or 'worker'"
    exit 1
fi
