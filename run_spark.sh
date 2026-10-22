#!/bin/bash
set -euo pipefail

#запуск от hadoop
if [ "$(id -un)" != "hadoop" ]; then
    exec sudo -H -u hadoop bash -s -- "$@" < "$0"
fi
cd /tmp

export SPARK_HOME=/opt/spark
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

APP="${1:-${SPARK_HOME}/examples/jars/spark-examples_2.12-3.5.7.jar}"

if [ ! -x "${SPARK_HOME}/bin/spark-submit" ]; then
    echo "Spark is not installed"
    exit 1
fi

if ! timeout 10 "${HADOOP_HOME}/bin/yarn" node -list > /dev/null 2>&1; then
    echo "YARN is unavailable. Check ResourceManager configuration and status."
    exit 1
fi

if [ "$#" -eq 0 ]; then
    "${SPARK_HOME}/bin/spark-submit" \
        --master yarn \
        --deploy-mode client \
        --conf spark.driver.host=10.26.0.10 \
        --conf spark.driver.bindAddress=10.26.0.10 \
        --class org.apache.spark.examples.SparkPi \
        "${APP}" 10
else
    shift
    "${SPARK_HOME}/bin/spark-submit" \
        --master yarn \
        --deploy-mode client \
        --conf spark.driver.host=10.26.0.10 \
        --conf spark.driver.bindAddress=10.26.0.10 \
        "${APP}" "$@"
fi
