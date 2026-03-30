#!/bin/bash

# start-spark.sh
# A script to start either a Spark Master or Spark Worker.

if [ "$SPARK_ROLE" == "master" ]; then
    echo "Starting Spark Master..."
    export SPARK_MASTER_HOST=0.0.0.0
    /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
elif [ "$SPARK_ROLE" == "worker" ]; then
    echo "Starting Spark Worker connecting to $SPARK_MASTER_URL ..."
    # The worker needs the master URL from the environment
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER_URL
else
    echo "SPARK_ROLE must be 'master' or 'worker'"
    exit 1
fi
