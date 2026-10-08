# Домашнее задание №1

Автоматизированное развёртывание HDFS-кластера с помощью Bash-скриптов.
- 1 NameNode;
- 1 Secondary NameNode;
- 3 DataNode.

## 2. Структура проекта

```text
hdfs-cluster/
├── deploy.sh
├── hadoop_setup_ssh.sh
├── install_hadoop.sh
├── install_hadoop_cluster.sh
├── hdfs_setup.sh
├── verify_cluster.sh
└── README.md
```




## Запуск 
```bash
./deploy.sh
```

Во время запуска создается пароль для пользователя `hadoop`.

После развёртывания:

```bash
./verify_cluster.sh
```

## Что делают скрипты

- `deploy.sh` — запускает развёртывание по порядку.
- `hadoop_setup_ssh.sh` — создаёт `hadoop` и настраивает SSH.
- `install_hadoop.sh` — устанавливает Java 17 и Hadoop 3.5.0.
- `install_hadoop_cluster.sh` — устанавливает Hadoop на все узлы.
- `hdfs_setup.sh` — создаёт конфигурацию и запускает HDFS.
- `verify_cluster.sh` — проверяет состояние кластера.
- `cleanup.sh` — останавливает HDFS и удаляет Hadoop и данные.

## Проверка

Проверяются:
- NameNode;
- SecondaryNameNode;
- 3 работающих DataNode;
- состояние HDFS;
- отсутствие Missing/Corrupt блоков;
- отсутствие `FATAL` и `CRITICAL` в логах.

NameNode UI:

http://team-26-nn:9870

## Результат

Кластер проверен:

- DataNode: `3/3`
- HDFS: `HEALTHY`
- Missing blocks: `0`
- Corrupt blocks: `0`
- Under-replicated blocks: `0`
- критических ошибок в логах нет.
