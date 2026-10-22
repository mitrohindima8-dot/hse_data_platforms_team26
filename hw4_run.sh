#!/bin/bash
set -e

#запускаем на en от team, после hw4_load_data.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JOB=/tmp/hw4_spark_job.py

#hadoop не видит ~team, кладем в /tmp
cp "$SCRIPT_DIR/hw4_spark_job.py" "$JOB"
chmod 644 "$JOB"
cd /tmp

#адрес resourcemanager на en
YARN_CONFIG=/opt/hadoop/etc/hadoop/yarn-site.xml
if ! grep -q "<name>yarn.resourcemanager.hostname</name>" "$YARN_CONFIG"; then
    sudo -u hadoop sed -i "/<\/configuration>/i\\
    <property>\\
        <name>yarn.resourcemanager.hostname</name>\\
        <value>team-26-nn</value>\\
    </property>" "$YARN_CONFIG"
fi

#запуск под yarn
#driver.host из-за 127.0.1.1 в hosts
sudo -u hadoop env HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
    /opt/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --name hw4_apartments \
    --driver-memory 512m \
    --num-executors 2 \
    --executor-memory 512m \
    --executor-cores 1 \
    --conf spark.yarn.am.memory=512m \
    --conf spark.driver.host=10.26.0.10 \
    --conf spark.driver.bindAddress=10.26.0.10 \
    --conf spark.pyspark.python=/usr/bin/python3 \
    "$JOB"

#проверка
echo "Задача в yarn:"
sudo -u hadoop /opt/hadoop/bin/yarn application -list -appStates FINISHED 2>/dev/null | grep hw4_apartments | tail -1
