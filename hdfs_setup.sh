#!/bin/bash

set -e

#запускаем на en от team, после hadoop_setup_ssh.sh и install_hadoop_cluster.sh

KEY="$HOME/.ssh/team_internal"
NAMENODE="team-26-nn"
SECONDARY_NAMENODE="team-26-00"
DATANODES=( "team-26-nn" "team-26-00" "team-26-01" )
NODES=( "team-26-nn" "team-26-00" "team-26-01" )

JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
HADOOP_HOME="/opt/hadoop"
HADOOP_CONF="$HADOOP_HOME/etc/hadoop"
DATA_DIR="/home/hadoop/hdfs"
REPLICATION=3

SSH_OPTS=( -i "$KEY" -o StrictHostKeyChecking=accept-new )

CONF_DIR=$(mktemp -d)
trap 'rm -rf "$CONF_DIR"' EXIT

#конфиги

cat > "$CONF_DIR/core-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://$NAMENODE:9000</value>
    </property>
</configuration>
EOF

cat > "$CONF_DIR/hdfs-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>$REPLICATION</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file://$DATA_DIR/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file://$DATA_DIR/data</value>
    </property>
    <property>
        <name>dfs.namenode.checkpoint.dir</name>
        <value>file://$DATA_DIR/namesecondary</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>$NAMENODE:9870</value>
    </property>
    <property>
        <name>dfs.namenode.rpc-bind-host</name>
        <value>0.0.0.0</value>
    </property>
    <property>
        <name>dfs.namenode.http-bind-host</name>
        <value>0.0.0.0</value>
    </property>
    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>$SECONDARY_NAMENODE:9868</value>
    </property>
</configuration>
EOF

printf '%s\n' "${DATANODES[@]}" > "$CONF_DIR/workers"

APPLY_CONF="
set -e
for f in core-site.xml hdfs-site.xml workers; do
    install -o hadoop -g hadoop -m 644 /tmp/hdfs-conf/\$f $HADOOP_CONF/\$f
done
rm -rf /tmp/hdfs-conf
sed -i '/^export JAVA_HOME=/d' $HADOOP_CONF/hadoop-env.sh
echo 'export JAVA_HOME=$JAVA_HOME' >> $HADOOP_CONF/hadoop-env.sh
"

#раскидываем по нодам

for node in "${NODES[@]}"; do
    #из-за 127.0.1.1 namenode слушала только localhost
    echo "$node: убираю 127.0.1.1 из /etc/hosts"
    ssh "${SSH_OPTS[@]}" team@"$node" "sudo sed -i -E 's/^(127\.0\.1\.1[[:space:]])/# \1/' /etc/hosts"

    echo "$node: копирую конфиги"
    ssh "${SSH_OPTS[@]}" team@"$node" "rm -rf /tmp/hdfs-conf && mkdir /tmp/hdfs-conf"
    scp "${SSH_OPTS[@]}" -q "$CONF_DIR"/* team@"$node":/tmp/hdfs-conf/
    ssh "${SSH_OPTS[@]}" team@"$node" "sudo bash -s" <<< "$APPLY_CONF"
    ssh "${SSH_OPTS[@]}" team@"$node" "sudo install -d -o hadoop -g hadoop -m 755 $DATA_DIR"
done

echo "edge: копирую конфиги (клиент)"
rm -rf /tmp/hdfs-conf && cp -r "$CONF_DIR" /tmp/hdfs-conf
sudo bash -s <<< "$APPLY_CONF"

#форматируем namenode только один раз

if ssh "${SSH_OPTS[@]}" team@"$NAMENODE" "sudo test -f $DATA_DIR/name/current/VERSION"; then
    echo "NameNode уже отформатирован, пропускаю"
else
    echo "Форматирую NameNode"
    ssh "${SSH_OPTS[@]}" team@"$NAMENODE" "sudo -u hadoop $HADOOP_HOME/bin/hdfs namenode -format -nonInteractive"
fi

#запуск, если уже запущен то перезапуск

echo "Запускаю HDFS"
ssh "${SSH_OPTS[@]}" team@"$NAMENODE" "sudo -u hadoop $HADOOP_HOME/sbin/stop-dfs.sh; sudo -u hadoop $HADOOP_HOME/sbin/start-dfs.sh"

#проверка

echo "Жду регистрации DataNode"
live=0
for _ in $(seq 1 12); do
    sleep 5
    live=$(ssh "${SSH_OPTS[@]}" team@"$NAMENODE" "sudo -u hadoop $HADOOP_HOME/bin/hdfs dfsadmin -report -live 2>/dev/null" \
        | sed -n 's/^Live datanodes (\([0-9]*\)):.*/\1/p')
    live=${live:-0}
    [ "$live" -ge "${#DATANODES[@]}" ] && break
done

for node in "${NODES[@]}"; do
    echo "$node: $(ssh "${SSH_OPTS[@]}" team@"$node" "sudo -u hadoop jps" | grep -v Jps | awk '{print $2}' | tr '\n' ' ')"
done

if ! ssh "${SSH_OPTS[@]}" team@"$SECONDARY_NAMENODE" "sudo -u hadoop jps" | grep -q SecondaryNameNode; then
    echo "ОШИБКА: не запустился SecondaryNameNode" >&2
    exit 1
fi

if [ "$live" -ge "${#DATANODES[@]}" ]; then
    echo "HDFS запущен: живых DataNode $live из ${#DATANODES[@]}"
else
    echo "ОШИБКА: живых DataNode $live из ${#DATANODES[@]}, смотрите логи в $HADOOP_HOME/logs" >&2
    exit 1
fi
