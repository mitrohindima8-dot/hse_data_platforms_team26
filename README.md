# ДЗ 2 — YARN

Развертывание YARN-кластера на виртуальных машинах команды 26.

## Состав кластера

- `team-26-en` — edge-нода
- `team-26-nn` — NameNode, DataNode, ResourceManager, NodeManager
- `team-26-00` — SecondaryNameNode, DataNode, NodeManager
- `team-26-01` — DataNode, NodeManager

В кластере работают 3 DataNode и 3 NodeManager.

## Скрипты

- `yarn_part1.sh` — настройка и запуск ResourceManager
- `yarn_part2.sh` — настройка и запуск NodeManager и публикация их веб-интерфейсов
- `yarn_part3.sh` — настройка MapReduce, запуск JobHistoryServer и тестовой MapReduce-задачи
- `deploy.sh` — общий скрипт развертывания и проверки кластера

## Запуск

Подключиться к edge-ноде:

```bash
ssh team@111.88.129.35
```

Перейти в репозиторий:

```bash
cd ~/hse_data_platforms_team26
```

Запустить:

```bash
./deploy.sh
```

Скрипт последовательно запускает ResourceManager, NodeManager и JobHistoryServer, выполняет тестовую MapReduce-задачу и проверяет состояние HDFS и YARN.

## Проверка

После успешного запуска:

- `hdfs dfsadmin -report` показывает 3 Live DataNode
- `yarn node -list` показывает 3 NodeManager в состоянии `RUNNING`
- тестовая MapReduce-задача завершается успешно
- NameNode UI доступен через порт `9870`

При успешном завершении `deploy.sh` выводит:

```text
Кластер развернут
```