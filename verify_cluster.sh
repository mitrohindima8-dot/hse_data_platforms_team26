#!/bin/bash

set -e

KEY="$HOME/.ssh/team_internal"

NAMENODE="team-26-nn"
SECONDARY="team-26-00"
NODES=("team-26-nn" "team-26-00" "team-26-01")

HADOOP_HOME="/opt/hadoop"

echo "Проверка NameNode"

if ssh -i "$KEY" team@"$NAMENODE" \
    "sudo -u hadoop jps | grep -q NameNode"; then
    echo "NameNode: OK"
else
    echo "ОШИБКА: NameNode не запущен"
    exit 1
fi

echo
echo "Проверка SecondaryNameNode"

if ssh -i "$KEY" team@"$SECONDARY" \
    "sudo -u hadoop jps | grep -q SecondaryNameNode"; then
    echo "SecondaryNameNode: OK"
else
    echo "ОШИБКА: SecondaryNameNode не запущен"
    exit 1
fi

echo
echo "Проверка DataNode"

REPORT=$(ssh -i "$KEY" team@"$NAMENODE" \
    "sudo -u hadoop $HADOOP_HOME/bin/hdfs dfsadmin -report")

LIVE=$(echo "$REPORT" |
    sed -n 's/^Live datanodes (\([0-9]*\)):.*/\1/p')

DEAD=$(echo "$REPORT" |
    sed -n 's/^Dead datanodes (\([0-9]*\)):.*/\1/p')

LIVE=${LIVE:-0}
DEAD=${DEAD:-0}

echo "Работает DataNode: $LIVE"
echo "Не работает DataNode: $DEAD"

if [ "$LIVE" -ne 3 ] || [ "$DEAD" -ne 0 ]; then
    echo "ОШИБКА: DataNode должны быть 3 и все должны работать"
    exit 1
fi

echo "DataNode: OK"

echo
echo "Проверка HDFS"

FSCK=$(ssh -i "$KEY" team@"$NAMENODE" \
    "sudo -u hadoop $HADOOP_HOME/bin/hdfs fsck / -blocks" 2>&1)

if ! echo "$FSCK" | grep -q "The filesystem under path '/' is HEALTHY"; then
    echo "ОШИБКА: HDFS не прошёл проверку"
    exit 1
fi

if ! echo "$FSCK" | grep -Eq 'Missing blocks:[[:space:]]*0'; then
    echo "ОШИБКА: есть отсутствующие блоки"
    exit 1
fi

if ! echo "$FSCK" | grep -Eq 'Corrupt blocks:[[:space:]]*0'; then
    echo "ОШИБКА: есть повреждённые блоки"
    exit 1
fi

echo "HDFS: OK"

echo
echo "Проверка логов"

for node in "${NODES[@]}"; do
    errors=$(ssh -i "$KEY" team@"$node" \
        "sudo grep -R -E 'FATAL|CRITICAL' $HADOOP_HOME/logs --include='*.log' 2>/dev/null" || true)

    if [ -n "$errors" ]; then
        echo "ОШИБКА: критические ошибки на $node"
        echo "$errors"
        exit 1
    fi
done

echo "Логи: OK"

echo
echo "Всё хорошо"