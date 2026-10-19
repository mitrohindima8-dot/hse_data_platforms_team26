#!/bin/bash
set -e

export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export HIVE_HOME=/opt/hive
export HIVE_CONF_DIR=/opt/hive/conf

if [ "$(id -un)" != "hadoop" ]; then
    exec sudo -u hadoop bash -s -- "$@" < "$0"
fi

JDBC_URL='jdbc:hive2://10.26.0.10:10000/default'

"$HIVE_HOME/bin/beeline" \
  -u "$JDBC_URL" \
  -n hadoop \
  --force=false \
  -e "SHOW DATABASES; SELECT 'client_1' AS client;" &
PID1=$!

"$HIVE_HOME/bin/beeline" \
  -u "$JDBC_URL" \
  -n hadoop \
  --force=false \
  -e "SHOW DATABASES; SELECT 'client_2' AS client;" &
PID2=$!

wait "$PID1"
echo "Подключения клиента1 выполнено"
wait "$PID2"

echo "Подключения клиента2 выполнено"
