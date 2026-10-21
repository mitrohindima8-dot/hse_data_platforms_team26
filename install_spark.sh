#!/bin/bash
set -euo pipefail

SPARK_VERSION=3.5.7
SPARK_PACKAGE="spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_ARCHIVE="${SPARK_PACKAGE}.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_ARCHIVE}"

SPARK_HOME=/opt/spark
HADOOP_HOME=/opt/hadoop
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"

if [ ! -x "${JAVA_HOME}/bin/java" ]; then
    echo "Java not found"
    exit 1
fi

if [ ! -x "${HADOOP_HOME}/bin/hadoop" ]; then
    echo "Hadoop not found"
    exit 1
fi

if [ ! -d "/opt/${SPARK_PACKAGE}" ]; then
    wget -O "/tmp/${SPARK_ARCHIVE}" "${SPARK_URL}"
    sudo tar -xzf "/tmp/${SPARK_ARCHIVE}" -C /opt
fi

sudo ln -sfn "/opt/${SPARK_PACKAGE}" "${SPARK_HOME}"

sudo tee "${SPARK_HOME}/conf/spark-env.sh" > /dev/null <<ENV
#!/bin/bash
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
export YARN_CONF_DIR=${HADOOP_CONF_DIR}
export PYSPARK_PYTHON=/usr/bin/python3
export SPARK_LOCAL_IP=10.26.0.10
ENV

sudo chmod 755 "${SPARK_HOME}/conf/spark-env.sh"

"${SPARK_HOME}/bin/spark-submit" --version

echo "Spark ${SPARK_VERSION} installed successfully"
