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

## Заметки (удалите потом пж)
### Важные
1. **После любой правки `pipeline.py` перезапускайте runner:** `sudo systemctl restart prefect-flow-team.service`. Без этого deployment продолжает работать на старой версии (так было у нас: `--test` проходил, а запуск через deployment падал). То же после повторного `--install`.
2. **NodeManager и JDK.** NodeManager на всех трёх узлах работал на удалённом JDK (после автообновления пакета) и не мог запускать контейнеры: `Failed to exec spawn helper` для `bash`. Лечится перезапуском NodeManager на каждом узле, мы это сделали. Автообновление JDK (`unattended-upgrades`) повторит проблему. Решать команде: `apt-mark hold openjdk-17-*` или плановый перезапуск NodeManager после обновлений.

### Не очень
  
4. **Spark в режиме `client`: адрес драйвера.** Воркеры должны подключаться к драйверу на `team-26-en`. Без явных `spark.driver.host` и `spark.driver.bindAddress` (оба `10.26.0.10`) AM падал с `Connection refused`. Эти параметры уже в `SPARK_CONF` в `pipeline.py`.
5. **Права в HDFS.** `team` может читать `/user/hadoop/...`, но не писать в `/user/hive/warehouse` и не заходить в `/tmp`. Поэтому результат пишется в `/user/team/`, а БД Hive создаётся с `LOCATION` и `MANAGEDLOCATION` в `/user/team/warehouse/`.
6. **Hive из Spark не используется.** Spark работает с `in-memory` каталогом (`spark.sql.catalogImplementation=in-memory`), потому что встроенный клиент Hive в Spark 3.5.7 (2.3.9) может быть несовместим с metastore 4.0.0-alpha-2. Таблицу создаёт Beeline через HiveServer2 поверх готового Parquet.
7. **Не делайте `SELECT COUNT(*)` и другие агрегации через Hive.** Движок Hive здесь `mr`: запрос запустит MapReduce-задачу, которой может не хватить памяти. Для проверки используйте `SELECT * ... LIMIT N` или считайте в Spark.
8. **Ресурсы YARN.** Узлы по 2048 МБ и 2 vcores, минимальная аллокация 1024 МБ: AM и каждый executor занимают по 1 GiB, всего 3 контейнера из 6 GiB. `maximum-am-resource-percent=0.1`, поэтому **запускайте один flow за раз**.
9. **Память на `team-26-en`:** 3.8 GiB, swap нет. Здесь одновременно работают Prefect Server, runner и драйвер Spark (512 МБ). Не запускайте на этом узле ничего тяжёлого во время прогона.
10. **Если `CREATE DATABASE` в Hive отказывает** (права или authorization), запустите flow с `hive_db=default`: БД создаваться не будет, таблица зарегистрируется в `default`.



