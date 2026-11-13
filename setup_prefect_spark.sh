#!/usr/bin/env bash
#
# setup_prefect_spark.sh  (v3)
# Prefect + Spark on YARN + HDFS + Hive на team-26-en.
#
# Запускать от пользователя "team", НЕ от root:
#   bash setup_prefect_spark.sh --check     # только чтение
#   bash setup_prefect_spark.sh --install   # установка (apt, venv, HDFS /user/team, 2 systemd-сервиса)
#   bash setup_prefect_spark.sh --test      # разовый прогон flow напрямую
#   bash setup_prefect_spark.sh --run [--param key=value ...]   # запуск через deployment (повторный запуск)
#   bash setup_prefect_spark.sh --status
#
# Что делает flow (apartments-etl-pipeline):
#   check-hive -> check-input -> prepare-hdfs -> spark-etl (YARN) -> register-hive-table -> verify-table
#
# Скрипт НЕ трогает /opt/spark/conf (общий для всех пользователей кластера):
# все параметры Spark передаются из pipeline.py явно.

set -Eeuo pipefail
IFS=$'\n\t'

readonly EXPECTED_USER="team"
readonly EXPECTED_HOST="team-26-en"
readonly HOME_DIR="/home/team"
readonly APP_DIR="${HOME_DIR}/prefect-spark"
readonly LOG_DIR="${APP_DIR}/logs"
readonly VENV_DIR="${HOME_DIR}/prefect-venv"
readonly PREFECT_HOME="${HOME_DIR}/.prefect"
readonly PREFECT_API_URL="http://127.0.0.1:4200/api"
readonly HADOOP_HOME="/opt/hadoop"
readonly HADOOP_CONF_DIR="/opt/hadoop/etc/hadoop"
readonly SPARK_HOME="/opt/spark"
readonly HIVE_HOME="/opt/hive"
readonly JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
readonly INPUT_CSV="/user/hadoop/hw4/input/apartments.csv"
readonly DEPLOYMENT="apartments-etl-pipeline/apartments-etl"

log()  { printf '\n[%s] %s\n' "$(date '+%F %T')" "$*"; }
warn() { printf '\n[WARN] %s\n' "$*" >&2; }
die()  { printf '\n[ERROR] %s\n' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

require_team() {
  [[ "$(id -un)" == "$EXPECTED_USER" ]] ||
    die "Запусти скрипт от пользователя ${EXPECTED_USER}, не от root."
  [[ "$(hostname -s)" == "$EXPECTED_HOST" ]] ||
    die "Ожидался узел ${EXPECTED_HOST}, текущий: $(hostname -s)."
}

backup_file() {
  local file="$1" stamp
  if sudo test -e "$file"; then
    stamp="$(date '+%Y%m%d-%H%M%S')"
    sudo cp -a "$file" "${file}.backup-${stamp}"
    log "Резервная копия: ${file}.backup-${stamp}"
  fi
}

prefect_env() {
  export PREFECT_HOME PREFECT_API_URL
  export JAVA_HOME HADOOP_HOME HADOOP_CONF_DIR SPARK_HOME
}

# --------------------------------------------------------------------------
check_local() {
  log "Проверка ОС и пользователя"
  . /etc/os-release
  printf 'OS: %s\n' "${PRETTY_NAME:-unknown}"
  printf 'Host: %s\nUser: %s\n' "$(hostname -s)" "$(id -un)"
  [[ "${ID:-}" == "ubuntu" ]] || die "Скрипт рассчитан на Ubuntu."

  log "Проверка Java / Hadoop / Spark / Hive"
  [[ -x "${HADOOP_HOME}/bin/hdfs" ]] || die "Не найден ${HADOOP_HOME}/bin/hdfs"
  [[ -x "${HADOOP_HOME}/bin/yarn" ]] || die "Не найден ${HADOOP_HOME}/bin/yarn"
  [[ -x "${SPARK_HOME}/bin/spark-submit" ]] || die "Не найден spark-submit"
  [[ -x "${HIVE_HOME}/bin/beeline" ]] || die "Не найден Beeline"
  java -version 2>&1 | head -n 3
  "${SPARK_HOME}/bin/spark-submit" --version 2>&1 | grep -E 'version|Scala version' | head -n 2 || true

  log "Проверка Python"
  python3 --version
  python3 - <<'PY'
import importlib.util
for module in ("venv", "ensurepip", "sqlite3", "ssl"):
    print(f"{module}: {'available' if importlib.util.find_spec(module) else 'missing'}")
PY
  if ! python3 -c 'import ensurepip' 2>/dev/null; then
    echo "ensurepip отсутствует: --install поставит python3.12-venv."
  fi
  if [[ -d "$VENV_DIR" && ! -x "${VENV_DIR}/bin/pip" ]]; then
    echo "${VENV_DIR} неполный (нет pip): --install переместит его в резервную копию и создаст заново."
  fi

  log "Проверка YARN"
  "${HADOOP_HOME}/bin/yarn" node -list

  log "Проверка HiveServer2 (только чтение)"
  timeout 30 "${HIVE_HOME}/bin/beeline" \
    -u 'jdbc:hive2://10.26.0.10:10000/default' -n team --silent=true \
    -e 'SHOW DATABASES;' || die "Не удалось выполнить SHOW DATABASES через HiveServer2."

  log "Проверка HDFS (только чтение)"
  "${HADOOP_HOME}/bin/hdfs" dfs -ls /user
  if "${HADOOP_HOME}/bin/hdfs" dfs -test -e /user/team; then
    "${HADOOP_HOME}/bin/hdfs" dfs -ls -d /user/team
  else
    echo "HDFS /user/team: отсутствует (будет создан в режиме --install)"
  fi
  if "${HADOOP_HOME}/bin/hdfs" dfs -test -e "$INPUT_CSV"; then
    "${HADOOP_HOME}/bin/hdfs" dfs -ls "$INPUT_CSV"
  else
    die "Входной файл ${INPUT_CSV} не найден в HDFS."
  fi

  log "Проверка свободного места и памяти"
  df -h "${HOME_DIR}"
  free -h
  log "Диагностика завершена. Изменений не выполнялось."
}

# --------------------------------------------------------------------------
write_app_files() {
  mkdir -p "$APP_DIR" "$LOG_DIR"
  chmod 750 "$APP_DIR" "$LOG_DIR"

  # ---------------- spark_etl.py ----------------
  cat > "${APP_DIR}/spark_etl.py" <<'PY'
"""Spark-задача: CSV из HDFS -> приведение типов -> агрегация -> Parquet в HDFS."""
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REQUIRED = {"id", "city", "rooms", "area", "price", "sale_date"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("apartments-etl").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = spark.read.option("header", "true").csv(args.input)
        missing = REQUIRED - set(raw.columns)
        if missing:
            raise ValueError(f"В CSV нет колонок: {sorted(missing)}; есть: {raw.columns}")

        # 1) Приведение типов
        typed = raw.select(
            F.col("id").cast("long").alias("id"),
            F.trim(F.col("city")).alias("city"),
            F.col("rooms").cast("int").alias("rooms"),
            F.col("area").cast("double").alias("area"),
            F.col("price").cast("long").alias("price"),
            F.to_date(F.col("sale_date"), "yyyy-MM-dd").alias("sale_date"),
        ).cache()
        rows_in = typed.count()

        # 2) Очистка: нет null, положительные значения, без дублей по id
        clean = (
            typed.where(
                F.col("id").isNotNull() & F.col("city").isNotNull()
                & F.col("rooms").isNotNull() & F.col("area").isNotNull()
                & F.col("price").isNotNull() & F.col("sale_date").isNotNull()
                & (F.col("area") > 0) & (F.col("price") > 0) & (F.col("rooms") > 0)
            )
            .dropDuplicates(["id"])
        )

        # 3) Производные колонки
        enriched = (
            clean
            .withColumn("price_per_m2", F.col("price") / F.col("area"))
            .withColumn("month", F.date_format(F.col("sale_date"), "yyyy-MM"))
        )

        # 4) Агрегация
        stats = (
            enriched.groupBy("city", "rooms", "month")
            .agg(
                F.count(F.lit(1)).alias("deals"),
                F.round(F.avg("price")).cast("long").alias("avg_price"),
                F.round(F.avg("price_per_m2"), 2).alias("avg_price_per_m2"),
                F.min("price").alias("min_price"),
                F.max("price").alias("max_price"),
            )
            .select("city", "rooms", "month", "deals", "avg_price",
                    "avg_price_per_m2", "min_price", "max_price")
            .orderBy("month", "city", "rooms")
        )

        # 5) Запись (overwrite => повторный запуск идемпотентен)
        stats.coalesce(1).write.mode("overwrite").parquet(args.output)

        back = spark.read.parquet(args.output)
        rows_out = back.count()
        rows_clean = clean.count()
        if rows_out == 0:
            raise RuntimeError("Результат пуст: в Parquet записано 0 строк")

        print("ETL_RESULT=" + json.dumps({
            "rows_in": rows_in,
            "rows_clean": rows_clean,
            "rows_dropped": rows_in - rows_clean,
            "rows_out": rows_out,
            "schema": back.dtypes,
            "output": args.output,
        }, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
PY

  # ---------------- pipeline.py ----------------
  cat > "${APP_DIR}/pipeline.py" <<'PY'
"""Prefect flow: CSV в HDFS -> Spark на YARN -> Parquet в HDFS -> внешняя таблица Hive."""
import json
import os
import posixpath
import re
import subprocess
import threading
from collections import deque
from datetime import datetime
from pathlib import Path

from prefect import flow, get_run_logger, task

APP_DIR = Path(__file__).resolve().parent
LOG_DIR = APP_DIR / "logs"

JAVA_HOME = "/usr/lib/jvm/java-17-openjdk-amd64"
HADOOP_HOME = "/opt/hadoop"
HDFS = f"{HADOOP_HOME}/bin/hdfs"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
BEELINE = "/opt/hive/bin/beeline"
HIVE_JDBC = "jdbc:hive2://10.26.0.10:10000/default"
NAMENODE = "hdfs://team-26-nn:9000"
HDFS_USER_DIR = "/user/team"

DEFAULT_INPUT = "/user/hadoop/hw4/input/apartments.csv"
DEFAULT_OUTPUT = f"{HDFS_USER_DIR}/team_prefect/apartments_stats"
DEFAULT_DB = "team_prefect"
DEFAULT_TABLE = "apartments_stats"

# Контейнеры YARN: узлы по 2048 MB, минимальная аллокация 1024 MB.
# AM (512+384) и executor'ы (512+384) округляются до 1 GiB: итого 3 контейнера.
SPARK_CONF = {
    "spark.executor.instances": "2",
    "spark.executor.cores": "1",
    "spark.executor.memory": "512m",
    "spark.executor.memoryOverhead": "384m",
    "spark.yarn.am.memory": "512m",
    "spark.yarn.am.memoryOverhead": "384m",
    "spark.yarn.maxAppAttempts": "1",
    "spark.dynamicAllocation.enabled": "false",
    "spark.pyspark.python": "/usr/bin/python3",
    "spark.sql.shuffle.partitions": "2",
    "spark.sql.catalogImplementation": "in-memory",
    "spark.ui.showConsoleProgress": "false",
}

KEY_MARKERS = ("ETL_RESULT", "Submitted application", "tracking URL",
               "final status", "ERROR", "Exception", "Traceback")


class PipelineError(RuntimeError):
    """Ошибка шага пайплайна с понятным сообщением."""


class CommandError(PipelineError):
    """Внешняя команда завершилась с ошибкой или по таймауту."""


def tool_env():
    env = os.environ.copy()
    for var in ("VIRTUAL_ENV", "PYTHONPATH", "PYTHONHOME"):
        env.pop(var, None)
    env.update({
        "JAVA_HOME": JAVA_HOME,
        "HADOOP_HOME": HADOOP_HOME,
        "HADOOP_CONF_DIR": f"{HADOOP_HOME}/etc/hadoop",
        "YARN_CONF_DIR": f"{HADOOP_HOME}/etc/hadoop",
        "SPARK_HOME": "/opt/spark",
        "HIVE_HOME": "/opt/hive",
        "PYSPARK_PYTHON": "/usr/bin/python3",
        "PATH": "/opt/spark/bin:/opt/hadoop/bin:/usr/local/bin:/usr/bin:/bin",
    })
    return env


def run_cmd(cmd, logger, name, timeout, tail=60):
    """Запускает команду, пишет весь вывод в файл, ключевые строки и хвост ошибки в логи Prefect."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{name}-{datetime.now():%Y%m%d-%H%M%S}.log"
    logger.info("Запуск [%s]: %s", name, " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, bufsize=1, env=tool_env())
    timed_out = threading.Event()

    def _kill():
        timed_out.set()
        proc.kill()

    timer = threading.Timer(timeout, _kill)
    timer.start()
    lines, last = [], deque(maxlen=tail)
    try:
        with open(log_path, "w", encoding="utf-8") as fh:
            for raw in proc.stdout:
                line = raw.rstrip("\n")
                fh.write(line + "\n")
                lines.append(line)
                last.append(line)
                if "SLF4J" not in line and any(m in line for m in KEY_MARKERS):
                    logger.info(line)
        rc = proc.wait()
    finally:
        timer.cancel()
        if proc.poll() is None:
            proc.kill()

    if timed_out.is_set():
        raise CommandError(f"[{name}] превышен таймаут {timeout} с. Лог: {log_path}")
    if rc != 0:
        logger.error("Последние строки вывода [%s]:\n%s", name, "\n".join(last))
        raise CommandError(f"[{name}] код возврата {rc}. Полный лог: {log_path}")
    logger.info("[%s] завершено успешно. Лог: %s", name, log_path)
    return lines


def ident(value, what):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", value):
        raise PipelineError(f"Недопустимое имя {what}: {value!r}")
    return value


def hdfs_path(path, what):
    if not re.fullmatch(r"/[A-Za-z0-9_./=-]+", path):
        raise PipelineError(f"{what}: нужен абсолютный путь HDFS из безопасных символов, получено {path!r}")
    return path


def beeline(statements, logger, name):
    cmd = [BEELINE, "-u", HIVE_JDBC, "-n", "team", "--silent=true",
           "--outputformat=tsv2", "-e", statements]
    lines = run_cmd(cmd, logger, name=name, timeout=300)
    return [ln for ln in lines if ln.strip() and "SLF4J" not in ln]


# ------------------------------------------------------------------ tasks
@task(name="check-hive", retries=2, retry_delay_seconds=10)
def check_hive():
    logger = get_run_logger()
    out = beeline("SHOW DATABASES;", logger, "hive-show-databases")
    logger.info("Базы Hive: %s", ", ".join(out[1:]) if len(out) > 1 else out)
    return out


@task(name="check-input", retries=0)
def check_input(input_path: str):
    logger = get_run_logger()
    hdfs_path(input_path, "input_path")
    try:
        run_cmd([HDFS, "dfs", "-test", "-e", input_path], logger, "hdfs-test", 60)
    except CommandError as exc:
        raise PipelineError(f"Входные данные не найдены или недоступны в HDFS: {input_path}") from exc
    lines = run_cmd([HDFS, "dfs", "-ls", input_path], logger, "hdfs-ls", 60)
    logger.info("Входные данные:\n%s", "\n".join(lines))
    return input_path


@task(name="prepare-hdfs", retries=1, retry_delay_seconds=10)
def prepare_hdfs(output_path: str, hive_db: str):
    logger = get_run_logger()
    hdfs_path(output_path, "output_path")
    ident(hive_db, "hive_db")
    dirs = [posixpath.dirname(output_path),
            f"{HDFS_USER_DIR}/warehouse/ext",
            f"{HDFS_USER_DIR}/warehouse/managed"]
    run_cmd([HDFS, "dfs", "-mkdir", "-p", *dirs], logger, "hdfs-mkdir", 60)
    return dirs


@task(name="spark-etl", retries=2, retry_delay_seconds=[30, 120])
def run_spark_etl(input_path: str, output_path: str):
    logger = get_run_logger()
    cmd = [SPARK_SUBMIT, "--master", "yarn", "--deploy-mode", "client",
           "--name", "prefect-apartments-etl",
           "--driver-memory", "512m"]
    for key, value in SPARK_CONF.items():
        cmd += ["--conf", f"{key}={value}"]
    cmd += [str(APP_DIR / "spark_etl.py"), "--input", input_path, "--output", output_path]
    lines = run_cmd(cmd, logger, "spark-submit", timeout=1200)

    result = None
    for line in lines:
        if line.startswith("ETL_RESULT="):
            result = json.loads(line[len("ETL_RESULT="):])
    if result is None:
        raise PipelineError("Spark завершился без строки ETL_RESULT: результат неизвестен")
    if result.get("rows_out", 0) <= 0:
        raise PipelineError(f"Spark записал 0 строк: {result}")
    logger.info("Spark: прочитано %s, после очистки %s (отброшено %s), строк в агрегате %s",
                result["rows_in"], result["rows_clean"], result["rows_dropped"], result["rows_out"])
    return result


@task(name="register-hive-table", retries=2, retry_delay_seconds=15)
def register_hive_table(output_path: str, hive_db: str, hive_table: str):
    logger = get_run_logger()
    db, table = ident(hive_db, "hive_db"), ident(hive_table, "hive_table")
    uri = f"{NAMENODE}{hdfs_path(output_path, 'output_path')}"
    statements = []
    if db != "default":
        statements.append(
            f"CREATE DATABASE IF NOT EXISTS {db} "
            f"LOCATION '{NAMENODE}{HDFS_USER_DIR}/warehouse/ext/{db}.db' "
            f"MANAGEDLOCATION '{NAMENODE}{HDFS_USER_DIR}/warehouse/managed/{db}.db';")
    statements.append(f"DROP TABLE IF EXISTS {db}.{table};")
    statements.append(
        f"CREATE EXTERNAL TABLE {db}.{table} ("
        "city STRING, rooms INT, month STRING, deals BIGINT, avg_price BIGINT, "
        "avg_price_per_m2 DOUBLE, min_price BIGINT, max_price BIGINT) "
        f"STORED AS PARQUET LOCATION '{uri}';")
    beeline(" ".join(statements), logger, "hive-register")
    logger.info("Таблица %s.%s зарегистрирована поверх %s", db, table, uri)
    return f"{db}.{table}"


@task(name="verify-table", retries=1, retry_delay_seconds=15)
def verify_table(full_name: str):
    logger = get_run_logger()
    out = beeline(f"SELECT * FROM {full_name} LIMIT 5;", logger, "hive-verify")
    rows = [ln for ln in out if "\t" in ln]
    logger.info("Пример строк из %s:\n%s", full_name, "\n".join(out))
    if len(rows) < 2:  # заголовок + хотя бы одна строка
        raise PipelineError(f"Таблица {full_name} пуста или не читается через Hive")
    return len(rows) - 1


# ------------------------------------------------------------------ flow
def notify_failure(flow, flow_run, state):
    print(f"[FAILED] flow run '{flow_run.name}': {state.message}")


@flow(name="apartments-etl-pipeline", log_prints=True, on_failure=[notify_failure],
      timeout_seconds=3600)
def apartments_etl_pipeline(
    input_path: str = DEFAULT_INPUT,
    output_path: str = DEFAULT_OUTPUT,
    hive_db: str = DEFAULT_DB,
    hive_table: str = DEFAULT_TABLE,
):
    logger = get_run_logger()
    logger.info("Старт: %s -> %s -> %s.%s", input_path, output_path, hive_db, hive_table)
    check_hive()
    check_input(input_path)
    prepare_hdfs(output_path, hive_db)
    etl = run_spark_etl(input_path, output_path)
    full_name = register_hive_table(output_path, hive_db, hive_table)
    sample = verify_table(full_name)
    logger.info("Готово: таблица %s, строк в агрегате %s", full_name, etl["rows_out"])
    return {"table": full_name, "rows_out": etl["rows_out"], "rows_in": etl["rows_in"],
            "rows_dropped": etl["rows_dropped"], "sample_rows": sample}


if __name__ == "__main__":
    apartments_etl_pipeline()
PY

  # ---------------- serve.py ----------------
  cat > "${APP_DIR}/serve.py" <<'PY'
"""Публикует deployment: flow можно запускать и перезапускать из UI, CLI или по расписанию."""
import os

from pipeline import apartments_etl_pipeline

if __name__ == "__main__":
    kwargs = {}
    cron = os.environ.get("ETL_CRON")  # например "0 3 * * *"
    if cron:
        kwargs["cron"] = cron
    apartments_etl_pipeline.serve(
        name="apartments-etl",
        tags=["spark", "yarn", "hive"],
        **kwargs,
    )
PY

  chmod 640 "${APP_DIR}/spark_etl.py" "${APP_DIR}/pipeline.py" "${APP_DIR}/serve.py"
  log "Созданы файлы приложения в ${APP_DIR}"
}

# --------------------------------------------------------------------------
hdfs_as_hadoop() {
  (cd / && sudo -u hadoop env \
    JAVA_HOME="$JAVA_HOME" \
    HADOOP_HOME="$HADOOP_HOME" \
    HADOOP_CONF_DIR="$HADOOP_CONF_DIR" \
    PATH="${HADOOP_HOME}/bin:/usr/bin:/bin" \
    "${HADOOP_HOME}/bin/hdfs" "$@")
}

install_unit() {
  local unit="/etc/systemd/system/$1" tmp
  tmp="$(mktemp)"
  cat > "$tmp"
  backup_file "$unit"
  sudo install -o root -g root -m 0644 "$tmp" "$unit"
  rm -f "$tmp"
}

ensure_venv() {
  if ! python3 -c 'import ensurepip' 2>/dev/null; then
    log "Устанавливаем python3.12-venv"
    sudo apt-get update
    sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y python3.12-venv
  fi
  python3 -c 'import ensurepip' || die "ensurepip всё ещё недоступен после установки python3.12-venv."

  if [[ -d "$VENV_DIR" && ! -x "${VENV_DIR}/bin/pip" ]]; then
    local stamp
    stamp="$(date '+%Y%m%d-%H%M%S')"
    warn "Неполный venv ${VENV_DIR} (нет pip). Переношу в ${VENV_DIR}.broken-${stamp}"
    mv "$VENV_DIR" "${VENV_DIR}.broken-${stamp}"
  fi
  [[ -d "$VENV_DIR" ]] || python3 -m venv "$VENV_DIR"

  "${VENV_DIR}/bin/python" -m pip install --upgrade pip
  "${VENV_DIR}/bin/python" -m pip install 'prefect>=3,<4'
  "${VENV_DIR}/bin/prefect" version
}

install_all() {
  require_team
  have sudo || die "sudo не найден."
  sudo -n true || die "Нужен sudo без запроса пароля."

  log "Проверяем свободное место"
  local avail_kb
  avail_kb="$(df -Pk "$HOME_DIR" | awk 'NR==2 {print $4}')"
  (( avail_kb > 2 * 1024 * 1024 )) || die "Меньше 2 GiB свободного места; установка остановлена."

  ensure_venv

  log "Проверяем HDFS home пользователя team"
  if "${HADOOP_HOME}/bin/hdfs" dfs -test -e /user/team; then
    local owner
    owner="$("${HADOOP_HOME}/bin/hdfs" dfs -ls -d /user/team | awk 'NR==1 {print $3}')"
    [[ "$owner" == "team" ]] ||
      die "/user/team уже существует, но владелец '${owner}', а не team. Ничего не меняю."
  else
    hdfs_as_hadoop dfs -mkdir /user/team
    hdfs_as_hadoop dfs -chown team:team /user/team
  fi
  "${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p \
    /user/team/team_prefect /user/team/warehouse/ext /user/team/warehouse/managed
  "${HADOOP_HOME}/bin/hdfs" dfs -ls /user/team

  write_app_files

  log "Проверяем синтаксис Python-файлов"
  "${VENV_DIR}/bin/python" - <<PY
import ast
for name in ("spark_etl.py", "pipeline.py", "serve.py"):
    ast.parse(open("${APP_DIR}/" + name, encoding="utf-8").read(), name)
    print("OK:", name)
PY

  log "Настраиваем Prefect CLI на локальный сервер"
  "${VENV_DIR}/bin/prefect" config set PREFECT_API_URL="$PREFECT_API_URL"

  log "Создаём systemd unit: Prefect Server"
  install_unit prefect-server-team.service <<'EOF'
[Unit]
Description=Prefect Server for user team
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=team
Group=team
WorkingDirectory=/home/team
Environment=PREFECT_HOME=/home/team/.prefect
Environment=PREFECT_SERVER_ANALYTICS_ENABLED=false
Environment=PYTHONUNBUFFERED=1
ExecStart=/home/team/prefect-venv/bin/prefect server start --host 127.0.0.1 --port 4200
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

  log "Создаём systemd unit: публикация deployment (prefect serve)"
  install_unit prefect-flow-team.service <<'EOF'
[Unit]
Description=Prefect deployment runner (apartments-etl) for user team
After=prefect-server-team.service
Wants=prefect-server-team.service

[Service]
Type=simple
User=team
Group=team
WorkingDirectory=/home/team/prefect-spark
Environment=PREFECT_HOME=/home/team/.prefect
Environment=PREFECT_API_URL=http://127.0.0.1:4200/api
Environment=JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
Environment=HADOOP_HOME=/opt/hadoop
Environment=HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
Environment=SPARK_HOME=/opt/spark
Environment=PYTHONUNBUFFERED=1
ExecStart=/home/team/prefect-venv/bin/python /home/team/prefect-spark/serve.py
Restart=on-failure
RestartSec=10
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

  sudo systemctl daemon-reload
  sudo systemctl enable --now prefect-server-team.service

  log "Ждём готовности Prefect API (до 2 минут)"
  local ready=0
  for _ in $(seq 1 60); do
    if curl -fsS "${PREFECT_API_URL}/health" >/dev/null 2>&1; then ready=1; break; fi
    sleep 2
  done
  (( ready == 1 )) || {
    sudo systemctl --no-pager --full status prefect-server-team.service || true
    die "Prefect API не ответил. Смотри: journalctl -u prefect-server-team.service -n 80"
  }

  sudo systemctl enable --now prefect-flow-team.service

  log "Ждём регистрации deployment ${DEPLOYMENT}"
  prefect_env
  local found=0
  for _ in $(seq 1 20); do
    if "${VENV_DIR}/bin/prefect" deployment inspect "$DEPLOYMENT" >/dev/null 2>&1; then found=1; break; fi
    sleep 3
  done
  if (( found == 1 )); then
    echo "Deployment зарегистрирован: ${DEPLOYMENT}"
  else
    warn "Deployment пока не виден. Смотри: journalctl -u prefect-flow-team.service -n 80"
  fi

  log "Установка завершена"
  echo "Prefect UI/API : http://127.0.0.1:4200 (с ноутбука: ssh -L 4200:127.0.0.1:4200 team@<узел>)"
  echo "Flow           : ${APP_DIR}/pipeline.py"
  echo "Дальше         : bash $0 --test   затем   bash $0 --run"
}

# --------------------------------------------------------------------------
run_test() {
  require_team
  [[ -x "${VENV_DIR}/bin/pip" ]] || die "Нет рабочего venv. Сначала выполни --install."
  [[ -f "${APP_DIR}/pipeline.py" && -f "${APP_DIR}/spark_etl.py" ]] ||
    die "Файлы flow отсутствуют. Сначала выполни --install."
  prefect_env
  curl -fsS "${PREFECT_API_URL}/health" >/dev/null ||
    die "Prefect Server не отвечает. Проверь: systemctl status prefect-server-team"
  log "Запускаем flow напрямую: Hive -> HDFS -> Spark/YARN -> таблица Hive"
  cd "$APP_DIR"
  "${VENV_DIR}/bin/python" pipeline.py
}

run_deployment() {
  require_team
  prefect_env
  log "Запускаем deployment ${DEPLOYMENT} (повторный запуск из CLI)"
  "${VENV_DIR}/bin/prefect" deployment run "$DEPLOYMENT" "$@"
}

show_status() {
  require_team
  prefect_env
  echo "===== Сервисы ====="
  systemctl --no-pager is-active prefect-server-team.service prefect-flow-team.service || true
  echo "===== Prefect API ====="
  curl -fsS "${PREFECT_API_URL}/health" || true
  echo
  echo "===== Последние запуски ====="
  "${VENV_DIR}/bin/prefect" flow-run ls --limit 5 2>&1 || true
  echo "===== HDFS ====="
  "${HADOOP_HOME}/bin/hdfs" dfs -ls -R /user/team/team_prefect 2>&1 | head -20 || true
  echo "===== Приложения YARN ====="
  "${HADOOP_HOME}/bin/yarn" application -list -appStates ALL 2>/dev/null | tail -5 || true
  echo "===== Логи шагов ====="
  ls -lt "$LOG_DIR" 2>/dev/null | head -8 || true
}

usage() {
  cat <<EOF
Использование: bash $0 [режим]

  --check             Только диагностика; ничего не изменяет.
  --install           Установка Prefect, HDFS /user/team, systemd-сервисов, flow.
  --test              Разовый запуск flow напрямую (виден в UI).
  --run [--param k=v] Запуск через deployment (повторный запуск, параметры).
  --status            Сервисы, последние запуски, результат в HDFS.
EOF
}

main() {
  require_team
  case "${1:---check}" in
    --check)   check_local ;;
    --install) install_all ;;
    --test)    run_test ;;
    --run)     shift; run_deployment "$@" ;;
    --status)  show_status ;;
    -h|--help) usage ;;
    *) usage; die "Неизвестный режим: $1" ;;
  esac
}
main "$@"
