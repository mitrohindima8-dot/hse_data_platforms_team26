#!/bin/bash

set -e

HADOOP_VERSION="3.5.0"
JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
HADOOP_HOME="/opt/hadoop"

sudo apt update
sudo apt install -y openjdk-17-jdk wget

if [ ! -d "$HADOOP_HOME" ]; then
    wget "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -O /tmp/hadoop.tar.gz
    sudo tar -xzf /tmp/hadoop.tar.gz -C /opt
    sudo mv "/opt/hadoop-${HADOOP_VERSION}" "$HADOOP_HOME"
    sudo chown -R team:team "$HADOOP_HOME"
    rm /tmp/hadoop.tar.gz
fi

sudo tee /etc/profile.d/hadoop.sh > /dev/null <<EOF
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
EOF

export JAVA_HOME="$JAVA_HOME"
export HADOOP_HOME="$HADOOP_HOME"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"

java -version
hadoop version
