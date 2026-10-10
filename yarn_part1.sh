#!/bin/bash
set -e

if [ "$(hostname)" != "team-26-nn" ]; then
    sudo -u hadoop ssh hadoop@team-26-nn 'bash -s' < "$0"
    exit $?
fi

HADOOP_HOME=/opt/hadoop
YARN_CONFIG="$HADOOP_HOME/etc/hadoop/yarn-site.xml"

if ! grep -q "<name>yarn.resourcemanager.hostname</name>" "$YARN_CONFIG"; then
    sed -i '/<\/configuration>/i\
    <property>\
        <name>yarn.resourcemanager.hostname</name>\
        <value>team-26-nn</value>\
    </property>' "$YARN_CONFIG"
fi

if ! grep -q "<name>yarn.resourcemanager.address</name>" "$YARN_CONFIG"; then
    sed -i '/<\/configuration>/i\
    <property>\
        <name>yarn.resourcemanager.address</name>\
        <value>team-26-nn:8032</value>\
    </property>' "$YARN_CONFIG"
fi

if ! grep -q "<name>yarn.resourcemanager.resource-tracker.address</name>" "$YARN_CONFIG"; then
    sed -i '/<\/configuration>/i\
    <property>\
        <name>yarn.resourcemanager.resource-tracker.address</name>\
        <value>team-26-nn:8031</value>\
    </property>' "$YARN_CONFIG"
fi

if ! grep -q "<name>yarn.resourcemanager.webapp.address</name>" "$YARN_CONFIG"; then
    sed -i '/<\/configuration>/i\
    <property>\
        <name>yarn.resourcemanager.webapp.address</name>\
        <value>team-26-nn:8088</value>\
    </property>' "$YARN_CONFIG"
fi

"$HADOOP_HOME/bin/yarn" --daemon start resourcemanager

sleep 5

if jps | grep -q ResourceManager; then
    echo "ResourceManager работает"
else
    echo "Ошибка запуска ResourceManager"
    exit 1
fi
curl -I http://team-26-nn:8088



