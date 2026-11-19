# setup_prefect_spark.sh: Prefect + Spark на YARN

Однофайловый скрипт: ставит Prefect на узел `team-26-en` и разворачивает flow, который читает CSV из HDFS, обрабатывает его в Spark на YARN и сохраняет результат как внешнюю таблицу Hive. Запускать от team на узле `team-26-en`.

## Что делает pipeline

```
check-hive -> check-input -> prepare-hdfs -> spark-etl (YARN) -> register-hive-table -> verify-table
```

| Шаг | Что происходит | Ретраи |
|---|---|---|
| `check-hive` | `SHOW DATABASES` через HiveServer2 (`10.26.0.10:10000`) | 2, пауза 10 с |
| `check-input` | входной файл есть в HDFS | нет (отсутствие файла не временный сбой) |
| `prepare-hdfs` | `mkdir -p` для выходного каталога и каталогов warehouse в `/user/team` | 1 |
| `spark-etl` | `spark-submit` на YARN: чтение CSV, приведение типов, обработка пропусков, очистка, агрегация, запись Parquet с партициями по `month` | 2, паузы 30 и 120 с |
| `register-hive-table` | создаёт БД и внешнюю партиционированную таблицу Hive поверх Parquet, `MSCK REPAIR` подхватывает партиции | 2, пауза 15 с |
| `verify-table` | `SHOW PARTITIONS` и `SELECT * ... LIMIT 5` через Hive, проверка, что партиции есть и таблица не пуста | 1 |

**Трансформации (`spark_etl.py`):**

- приведение типов: `id`, `rooms`, `area`, `price`, `sale_date`
- обработка пропусков: число пустых значений по каждой колонке пишется в лог, пустой `city` заменяется на `unknown`, строки без `price`, `area`, `rooms` или `sale_date` убираются
- очистка: только положительные `area`, `price`, `rooms`, дедупликация по `id`
- новые колонки: цена за м² и месяц продажи
- агрегация по `city, rooms, month`: число сделок, средняя цена, средняя цена за м², min, max

Результат пишется в Parquet с партициями по `month` (`partitionBy("month")`, одна папка `month=YYYY-MM` на месяц) в режиме `overwrite`, поэтому повторный запуск идемпотентен.

На тестовых данных из ДЗ 4: прочитано 5000 строк, пропусков в `price` 50, после очистки 4940, в агрегате 180 строк, 9 партиций `month=2026-01` ... `month=2026-09`.


## Что создаётся

| Что | Где |
|---|---|
| Файлы flow | `/home/team/prefect-spark/` (`pipeline.py`, `spark_etl.py`, `serve.py`) |
| Логи шагов | `/home/team/prefect-spark/logs/` (полный вывод каждой команды) |
| Python-окружение с Prefect 3 | `/home/team/prefect-venv` |
| Данные Prefect (SQLite) | `/home/team/.prefect` |
| systemd: Prefect Server | `prefect-server-team.service` (`127.0.0.1:4200`) |
| systemd: runner с deployment | `prefect-flow-team.service` (deployment `apartments-etl`) |
| HDFS | `/user/team` (владелец `team`), `/user/team/team_prefect/apartments_stats` (Parquet, партиции `month=YYYY-MM`), `/user/team/warehouse/{ext,managed}` |
| Hive | БД `team_prefect`, внешняя таблица `team_prefect.apartments_stats`, партиционирована по `month` |


## Режимы

```bash
bash setup_prefect_spark.sh --check     # только чтение: ОС, Java/Hadoop/Spark/Hive, YARN, HDFS, входной файл
bash setup_prefect_spark.sh --install   # установка (идемпотентна, можно повторять)
bash setup_prefect_spark.sh --test      # разовый прогон flow напрямую из терминала
bash setup_prefect_spark.sh --run       # запуск через deployment (повторный запуск)
bash setup_prefect_spark.sh --status    # сервисы, последние запуски, результат в HDFS
```


## Быстрый старт

```bash
bash ~/setup_prefect_spark.sh --check
bash ~/setup_prefect_spark.sh --install
bash ~/setup_prefect_spark.sh --test
bash ~/setup_prefect_spark.sh --run
```

## Prefect UI

Сервер слушает только `127.0.0.1`. С вашего компьютера откройте SSH-туннель и зайдите на http://127.0.0.1:4200:

```bash
ssh -L 4200:127.0.0.1:4200 team@<адрес-узла>
```

## Параметры flow

Параметры flow `apartments-etl-pipeline` и их значения по умолчанию:

| Параметр | По умолчанию |
|---|---|
| `input_path` | `/user/hadoop/hw4/input/apartments.csv` |
| `output_path` | `/user/team/team_prefect/apartments_stats` |
| `hive_db` | `team_prefect` |
| `hive_table` | `apartments_stats` |

Можно менять в UI («Run» -> «Custom run») или через CLI: `bash setup_prefect_spark.sh --run --param hive_db=default` (запуск с `--param` не проверялся). Расписание: добавить `Environment=ETL_CRON=0 3 * * *` в `prefect-flow-team.service`, затем `sudo systemctl daemon-reload && sudo systemctl restart prefect-flow-team.service`.

## Проверка таблицы и данных

`hw5_check.sh` проверяет результат обработки после завершения Flow. Скрипт читает таблицу `team_prefect.apartments_stats` через Beeline и исходный CSV из HDFS.

В Python повторяется очистка исходных данных и рассчитываются число сделок и средняя цена по группам `month, city, rooms`. Полученные значения сравниваются с таблицей Hive. Также проверяется совпадение списков партиций в исходных данных, Hive и HDFS.

Запуск на `team-26-en` от пользователя `team`:

```bash
bash hw5_check.sh
```

При несовпадении данных или партиций скрипт завершается с ошибкой. Временные файлы выгрузки удаляются после проверки.

## Результат проверки

- таблица `team_prefect.apartments_stats` читается через Beeline;
- число сделок и средняя цена совпадают с исходными данными во всех 180 группах;
- после очистки — 4940 сделок;
- в Hive и HDFS совпадают 9 партиций: `month=2026-01` ... `month=2026-09`.

Вывод скрипта:

```text
Сделок после очистки в CSV: 4940 ; в Hive: 4940
Групп в CSV: 180 ; в Hive: 180
Партиций: ожидается 9 ; Hive 9 ; HDFS 9
Проверка пройдена: Hive читает таблицу, данные и партиции совпадают
```
