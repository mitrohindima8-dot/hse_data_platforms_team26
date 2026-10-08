#!/bin/bash

set -e

echo "Настройка пользователей, ssh"
./hadoop_setup_ssh.sh

echo "Установка Java, установка Hadoop"
./install_hadoop_cluster.sh

echo "Настройка и запуск HDFS"
./hdfs_setup.sh

echo "Завершено"