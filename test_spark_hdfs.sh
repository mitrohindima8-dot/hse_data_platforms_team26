#!/bin/bash
set -euo pipefail

SPARK_HOME=/opt/spark
HADOOP_HOME=/opt/hadoop
HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$(id -un)" != "team" ]; then
    echo "Run this script as team"
    exit 1
fi

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

printf 'apple\nbanana\napple\norange\n' > "${TMP_DIR}/spark_input.txt"
cp "${SCRIPT_DIR}/test_spark_hdfs.py" "${TMP_DIR}/test_spark_hdfs.py"

chmod 755 "${TMP_DIR}"
chmod 644 "${TMP_DIR}/spark_input.txt" "${TMP_DIR}/test_spark_hdfs.py"

sudo -u hadoop env \
    HADOOP_HOME="${HADOOP_HOME}" \
    HADOOP_CONF_DIR="${HADOOP_CONF_DIR}" \
    YARN_CONF_DIR="${HADOOP_CONF_DIR}" \
    bash -c '
        set -e
        /opt/hadoop/bin/hdfs dfs -mkdir -p /user/hadoop/spark_test
        /opt/hadoop/bin/hdfs dfs -put -f "$1/spark_input.txt" /user/hadoop/spark_test/spark_input.txt
        /opt/spark/bin/spark-submit --master "local[2]" "$1/test_spark_hdfs.py"
    ' _ "${TMP_DIR}"
