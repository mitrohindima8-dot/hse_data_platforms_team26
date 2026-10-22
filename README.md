# ДЗ 4. Apache Spark под управлением YARN

Spark-приложение запускается под YARN на кластере из прошлых заданий, читает данные из HDFS, преобразует их и сохраняет в партиционированную таблицу, которую читает Hive.

## Что нужно до запуска

- ДЗ 1: HDFS (NameNode на team-26-nn, 3 DataNode)
- ДЗ 2: YARN (ResourceManager на team-26-nn, 3 NodeManager)
- ДЗ 3: Hive 4.0.0-alpha2 на team-26-en (Metastore на PostgreSQL, HiveServer2 на 10.26.0.10:10000)

## Состав кластера

| Нода | Что запущено |
|------|--------------|
| team-26-en | edge, Spark 3.5.7, Hive Metastore, HiveServer2, PostgreSQL |
| team-26-nn | NameNode, DataNode, ResourceManager, NodeManager, JobHistoryServer |
| team-26-00 | Secondary NameNode, DataNode, NodeManager |
| team-26-01 | DataNode, NodeManager |

Spark стоит только на en. Драйвер работает на en (режим client), executor запускаются в контейнерах YARN на nn, 00, 01.

## Скрипты

| Файл | Что делает |
|------|------------|
| `install_spark.sh` | скачивает Spark 3.5.7 в `/opt/spark`, пишет `spark-env.sh` (JAVA_HOME, HADOOP_CONF_DIR, YARN_CONF_DIR), прописывает на en адрес ResourceManager |
| `run_spark.sh` | проверяет, что YARN доступен, и запускает SparkPi под YARN. Можно передать свое приложение: `bash run_spark.sh /tmp/app.py` |
| `test_spark_hdfs.sh`, `test_spark_hdfs.py` | кладут тестовый файл в HDFS и проверяют, что Spark его читает |
| `hw4_load_data.sh` | генерирует CSV с продажами квартир (5000 строк, 5 городов, январь-сентябрь 2026, есть битые строки) и кладет в HDFS `/user/hadoop/hw4/input/apartments.csv` |
| `hw4_spark_job.py` | Spark-приложение: чтение из HDFS, трансформации, запись таблицы `hw4.apartments_stats` с партициями |
| `hw4_run.sh` | запускает `hw4_spark_job.py` через `spark-submit --master yarn` |
| `hw4_check.sh` | читает таблицу через beeline (клиент Hive) и сверяет ее с исходным CSV |

`install_hive.sh`, `two_clients.sh`, `sql_table_check.sh` остались из ДЗ 3, они нужны, чтобы работал Hive.

## Запуск

Все скрипты запускаются на en от пользователя team.

```bash
ssh team@111.88.129.35
git clone -b HW-4-spark https://github.com/mitrohindima8-dot/hse_data_platforms_team26.git hw4_repo
cd hw4_repo
```

Установка Spark и проверка, что он работает под YARN и видит HDFS:

```bash
bash install_spark.sh
bash run_spark.sh
bash test_spark_hdfs.sh
```

Обработка данных и таблица в Hive:

```bash
bash hw4_load_data.sh
bash hw4_run.sh
bash hw4_check.sh
```

## Что делает Spark-приложение

1. Создает сессию Spark с поддержкой Hive (`enableHiveSupport`, metastore `thrift://localhost:9083`). Master `yarn` задается в `hw4_run.sh`.
2. Читает CSV из HDFS: `hdfs:///user/hadoop/hw4/input/apartments.csv`.
3. Трансформации:
   - приведение типов: id, rooms в int, area в double, price в long, sale_date в date
   - очистка: убираются строки без цены и с площадью <= 0
   - новые колонки: цена за м2 (`price_per_m2`) и месяц продажи (`month`)
   - агрегация по месяцу, городу и числу комнат: число сделок, средняя цена, средняя цена за м2, мин и макс цена
4. Сохраняет результат как таблицу Hive `hw4.apartments_stats` в формате parquet с партициями по месяцу (`partitionBy("month")`). Данные лежат в HDFS `/user/hadoop/hw4/apartments_stats`.

## Соответствие заданию

| Пункт | Где сделано |
|-------|-------------|
| 1. Сессия Spark под управлением YARN | `hw4_run.sh`, `run_spark.sh`: `--master yarn` |
| 2. Подключение к HDFS | `HADOOP_CONF_DIR` в spark-env.sh, чтение `hdfs:///...` |
| 3. Чтение данных, загруженных на HDFS | `hw4_load_data.sh` кладет CSV, `hw4_spark_job.py` читает |
| 4. Несколько трансформаций | приведение типов, фильтрация, новые колонки, агрегация |
| 5. Партиционирование при сохранении | `partitionBy("month")` |
| 6. Сохранение как таблицы | `saveAsTable("hw4.apartments_stats")` |
| 7. Чтение стандартным клиентом Hive | `hw4_check.sh` через beeline |

## Проверка

`hw4_check.sh` читает таблицу через beeline, выгружает ее и сравнивает с исходным CSV: число сделок и средняя цена в каждой группе, число групп, число партиций.

Вручную:

```bash
sudo -u hadoop /opt/hive/bin/beeline -u jdbc:hive2://10.26.0.10:10000/default -n hadoop -e "SHOW PARTITIONS hw4.apartments_stats; SELECT * FROM hw4.apartments_stats WHERE month='2026-03' LIMIT 10;"
```

Задача в YARN:

```bash
sudo -u hadoop /opt/hadoop/bin/yarn application -list -appStates FINISHED
```

## Результат

- `run_spark.sh`: `Pi is roughly 3.14...`
- `test_spark_hdfs.sh`: `Spark HDFS test PASSED`
- `hw4_run.sh`: задача `hw4_apartments` в YARN со статусом SUCCEEDED, из 5000 строк после очистки 4940
- таблица `hw4.apartments_stats`: 9 партиций, `month=2026-01` ... `month=2026-09`, 180 строк
- `hw4_check.sh`: `Проверка пройдена: hive читает таблицу, данные и партиции совпадают`

## Замечания

- на en в `yarn-site.xml` не было адреса ResourceManager, и Spark стучался в 0.0.0.0:8032. Теперь `install_spark.sh` прописывает `yarn.resourcemanager.hostname=team-26-nn`
- на en в /etc/hosts есть строка 127.0.1.1 team-26-en, поэтому драйверу явно задан адрес 10.26.0.10 (`spark.driver.host`), иначе контейнеры на нодах не могут к нему подключиться
- `run_spark.sh` переключается на пользователя hadoop: у team нет прав на запись в HDFS `/user`
- запросы с GROUP BY в Hive сейчас падают (MapReduce в Hive 4 на Java 17), поэтому агрегаты считаются в Spark, а Hive проверяется чтением таблицы
