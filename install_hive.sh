
#!/usr/bin/env bash
# install_hive.sh
# Apache Hive 4.0.0-alpha-2 + PostgreSQL Metastore + HiveServer2
# Target: team-26-en, Ubuntu, Hadoop 3.5.0, Java 17

set -Eeuo pipefail
umask 027

HIVE_VERSION="4.0.0-alpha-2"
HIVE_ARCHIVE="apache-hive-${HIVE_VERSION}-bin.tar.gz"
HIVE_URL="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${HIVE_ARCHIVE}"

HIVE_HOME="/opt/hive"
HADOOP_HOME="/opt/hadoop"
HADOOP_CONF_DIR="/opt/hadoop/etc/hadoop"
JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

DB_NAME="hive_metastore"
DB_USER="hive"
DB_PASSWORD_FILE="/etc/hive/metastore-db-password"

HIVE_HOST="10.26.0.10"
METASTORE_PORT="9083"
HIVESERVER2_PORT="10000"

JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED"

log() {
    echo
    echo "==== $* ===="
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

# -------------------------------------------------------------------
# 0. Предварительные проверки
# -------------------------------------------------------------------

[[ "$(id -u)" -ne 0 ]] \
    || die "Запусти скрипт от пользователя team, не от root."

sudo -n true \
    || die "Для пользователя team необходим sudo без запроса пароля."

[[ "$(hostname)" == "team-26-en" ]] \
    || die "Скрипт предназначен для team-26-en."

for cmd in awk curl openssl tar timeout; do
    command -v "$cmd" >/dev/null 2>&1 \
        || die "Не найдена команда: $cmd"
done

[[ -x "$HADOOP_HOME/bin/hdfs" ]] \
    || die "Не найден Hadoop: $HADOOP_HOME"

[[ -d "$HADOOP_CONF_DIR" ]] \
    || die "Не найдена конфигурация Hadoop: $HADOOP_CONF_DIR"

[[ -x "$JAVA_HOME/bin/java" ]] \
    || die "Не найден Java: $JAVA_HOME"

id hadoop >/dev/null 2>&1 \
    || die "Не найден системный пользователь hadoop"

if [[ -e "$HIVE_HOME" && ! -x "$HIVE_HOME/bin/hive" ]]; then
    die "$HIVE_HOME существует, но исполняемый файл Hive не найден. Проверь каталог вручную."
fi

# Не изменяем конфигурацию существующего Hadoop-кластера.
log "1. Установка системных пакетов"

sudo apt-get update

sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y \
    postgresql \
    postgresql-client \
    libpostgresql-jdbc-java \
    curl \
    ca-certificates \
    tar \
    openssl

sudo systemctl enable --now postgresql
sudo systemctl is-active --quiet postgresql \
    || die "PostgreSQL не запущен."

# -------------------------------------------------------------------
# 2. PostgreSQL и пароль Metastore
# -------------------------------------------------------------------

log "2. Настройка PostgreSQL"

sudo install -d -o root -g hadoop -m 0750 /etc/hive

if ! sudo test -s "$DB_PASSWORD_FILE"; then
    sudo sh -c \
        'umask 077; openssl rand -hex 24 > /etc/hive/metastore-db-password'
fi

sudo chown root:hadoop "$DB_PASSWORD_FILE"
sudo chmod 0640 "$DB_PASSWORD_FILE"

DB_PASSWORD="$(sudo cat "$DB_PASSWORD_FILE")"
[[ -n "$DB_PASSWORD" ]] || die "Пароль Metastore пуст."

# Создаём роль, если её нет. При повторном запуске синхронизируем
# пароль роли с защищённым файлом. Существующие базы не удаляем.
sudo -u postgres psql -v ON_ERROR_STOP=1 \
    -v db_password="$DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE hive LOGIN PASSWORD %L', :'db_password')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'hive'
)
\gexec

SELECT format('ALTER ROLE hive WITH LOGIN PASSWORD %L', :'db_password')
\gexec

SELECT 'CREATE DATABASE hive_metastore OWNER hive'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = 'hive_metastore'
)
\gexec
SQL

# Не меняем владельца существующей базы автоматически.
# Проверяем подключение отдельной учётной записью Metastore.
PGPASSWORD="$DB_PASSWORD" psql \
    -h 127.0.0.1 \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -v ON_ERROR_STOP=1 \
    -c 'SELECT 1' >/dev/null \
    || die "Не удалось подключиться к PostgreSQL как $DB_USER."

# -------------------------------------------------------------------
# 3. Установка Hive
# -------------------------------------------------------------------

log "3. Установка Apache Hive"

if [[ ! -x "$HIVE_HOME/bin/hive" ]]; then
    FREE_KB="$(df -Pk /opt | awk 'NR==2 {print $4}')"

    [[ "$FREE_KB" =~ ^[0-9]+$ ]] \
        || die "Не удалось определить свободное место в /opt."

    [[ "$FREE_KB" -ge 5242880 ]] \
        || die "В /opt необходимо не менее 5 GiB свободного места."

    curl -fL --retry 3 "$HIVE_URL" -o "/tmp/$HIVE_ARCHIVE"

    tar -tzf "/tmp/$HIVE_ARCHIVE" >/dev/null \
        || die "Загруженный архив Hive повреждён."

    EXTRACT_DIR="$(mktemp -d /tmp/hive-extract.XXXXXX)"
    trap 'rm -rf -- "$EXTRACT_DIR"' EXIT

    tar -xzf "/tmp/$HIVE_ARCHIVE" -C "$EXTRACT_DIR"

    [[ -x "$EXTRACT_DIR/apache-hive-${HIVE_VERSION}-bin/bin/hive" ]] \
        || die "В архиве не найден исполняемый файл Hive."

    sudo mv "$EXTRACT_DIR/apache-hive-${HIVE_VERSION}-bin" "$HIVE_HOME"
    rmdir "$EXTRACT_DIR"
    trap - EXIT
    rm -f "/tmp/$HIVE_ARCHIVE"
else
    echo "Hive уже установлен: $HIVE_HOME"
fi

[[ -f /usr/share/java/postgresql.jar ]] \
    || die "Не найден JDBC-драйвер /usr/share/java/postgresql.jar"

sudo install -o hadoop -g hadoop -m 0644 \
    /usr/share/java/postgresql.jar \
    "$HIVE_HOME/lib/postgresql-jdbc.jar"

sudo chown -R hadoop:hadoop "$HIVE_HOME"
sudo chmod -R a+rX "$HIVE_HOME"

# -------------------------------------------------------------------
# 4. Конфигурация Hive
# -------------------------------------------------------------------

log "4. Настройка hive-site.xml"

sudo tee "$HIVE_HOME/conf/hive-site.xml" >/dev/null <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://127.0.0.1:5432/${DB_NAME}</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>${DB_USER}</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>${DB_PASSWORD}</value>
  </property>

  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://127.0.0.1:${METASTORE_PORT}</value>
  </property>

  <property>
    <name>hive.metastore.thrift.bind.host</name>
    <value>127.0.0.1</value>
  </property>

  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>hdfs:///user/hive/warehouse</value>
  </property>

  <property>
    <name>hive.server2.thrift.bind.host</name>
    <value>${HIVE_HOST}</value>
  </property>

  <property>
    <name>hive.server2.thrift.port</name>
    <value>${HIVESERVER2_PORT}</value>
  </property>

  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
  </property>

  <property>
    <name>hive.server2.authentication</name>
    <value>NONE</value>
  </property>

  <property>
    <name>hive.notification.event.poll.interval</name>
    <value>0</value>
  </property>
</configuration>
EOF

sudo chown hadoop:hadoop "$HIVE_HOME/conf/hive-site.xml"
sudo chmod 0640 "$HIVE_HOME/conf/hive-site.xml"

# Проверяем XML штатным средством Hadoop, не меняя конфиги кластера.
sudo -u hadoop env \
    JAVA_HOME="$JAVA_HOME" \
    HADOOP_HOME="$HADOOP_HOME" \
    HADOOP_CONF_DIR="$HADOOP_CONF_DIR" \
    "$HADOOP_HOME/bin/hadoop" conftest \
    -conffile "$HIVE_HOME/conf/hive-site.xml" \
    || die "Проверка XML конфигурации не прошла."

# -------------------------------------------------------------------
# 5. Каталоги HDFS
# -------------------------------------------------------------------

log "5. Подготовка HDFS warehouse"

HDFS_CMD=(
    sudo -u hadoop env
    "HADOOP_HOME=$HADOOP_HOME"
    "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
    "JAVA_HOME=$JAVA_HOME"
    "PATH=$HADOOP_HOME/bin:$JAVA_HOME/bin:/usr/bin:/bin"
    "$HADOOP_HOME/bin/hdfs"
)

"${HDFS_CMD[@]}" dfs -mkdir -p /user/hive/warehouse

# Не используем -R: существующие данные не затрагиваем.
"${HDFS_CMD[@]}" dfs -chown hadoop:supergroup \
    /user/hive /user/hive/warehouse

"${HDFS_CMD[@]}" dfs -chmod 775 \
    /user/hive /user/hive/warehouse

# -------------------------------------------------------------------
# 6. Инициализация схемы Metastore
# -------------------------------------------------------------------

log "6. Инициализация схемы PostgreSQL Metastore"

VERSION_TABLE="$(
    sudo -u postgres psql -d "$DB_NAME" -tAc \
        "SELECT to_regclass('public.\"VERSION\"')"
)"

TABLE_COUNT="$(
    sudo -u postgres psql -d "$DB_NAME" -tAc \
        "SELECT count(*) FROM information_schema.tables
         WHERE table_schema = 'public'
         AND table_type = 'BASE TABLE'"
)"

SCHEMA_ENV=(
    "HIVE_HOME=$HIVE_HOME"
    "HIVE_CONF_DIR=$HIVE_HOME/conf"
    "HADOOP_HOME=$HADOOP_HOME"
    "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
    "JAVA_HOME=$JAVA_HOME"
    "JAVA_TOOL_OPTIONS=$JAVA_OPTS"
    "PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$JAVA_HOME/bin:/usr/bin:/bin"
)

if [[ -z "${VERSION_TABLE//[[:space:]]/}" ]]; then
    if [[ "$TABLE_COUNT" -gt 0 ]]; then
        die "В базе есть таблицы, но отсутствует public."VERSION". Возможна частичная инициализация; проверь базу вручную."
    fi

    sudo -u hadoop env "${SCHEMA_ENV[@]}" \
        "$HIVE_HOME/bin/schematool" \
        -dbType postgres -initSchema
else
    echo "Схема уже существует; проверяем её состояние."

    sudo -u hadoop env "${SCHEMA_ENV[@]}" \
        "$HIVE_HOME/bin/schematool" \
        -dbType postgres -info
fi

# -------------------------------------------------------------------
# 7. systemd
# -------------------------------------------------------------------

log "7. Создание systemd-сервисов"

sudo tee /etc/systemd/system/hive-metastore.service >/dev/null <<'EOF'
[Unit]
Description=Apache Hive Metastore
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=hadoop
Group=hadoop
Environment=JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
Environment=HADOOP_HOME=/opt/hadoop
Environment=HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
Environment=HIVE_HOME=/opt/hive
Environment=HIVE_CONF_DIR=/opt/hive/conf
Environment="JAVA_TOOL_OPTIONS=--add-opens=java.base/java.nio=ALL-UNNAMED"
WorkingDirectory=/opt/hive
ExecStart=/opt/hive/bin/hive --service metastore
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/hive-server2.service >/dev/null <<'EOF'
[Unit]
Description=Apache HiveServer2
After=network.target hive-metastore.service
Requires=hive-metastore.service

[Service]
Type=simple
User=hadoop
Group=hadoop
Environment=JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
Environment=HADOOP_HOME=/opt/hadoop
Environment=HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
Environment=HIVE_HOME=/opt/hive
Environment=HIVE_CONF_DIR=/opt/hive/conf
Environment="JAVA_TOOL_OPTIONS=--add-opens=java.base/java.nio=ALL-UNNAMED"
WorkingDirectory=/opt/hive
ExecStart=/opt/hive/bin/hive --service hiveserver2
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemd-analyze verify \
    /etc/systemd/system/hive-metastore.service \
    /etc/systemd/system/hive-server2.service \
    || die "Проверка systemd unit-файлов не прошла."

sudo systemctl daemon-reload
sudo systemctl enable hive-metastore hive-server2

# -------------------------------------------------------------------
# 8. Ожидание запуска служб
# -------------------------------------------------------------------

wait_for_port() {
    local port="$1"
    local service="$2"

    for _ in $(seq 1 60); do
        if sudo systemctl is-active --quiet "$service"; then
            if sudo ss -lnt "( sport = :$port )" | grep -q LISTEN; then
                return 0
            fi
        else
            sudo journalctl -u "$service" -n 60 --no-pager
            return 1
        fi

        sleep 2
    done

    echo "Не дождались порта $port для сервиса $service."
    sudo journalctl -u "$service" -n 80 --no-pager
    return 1
}

log "8. Запуск Metastore"

sudo systemctl restart hive-metastore

wait_for_port "$METASTORE_PORT" hive-metastore \
    || die "Hive Metastore не запустился."

log "9. Запуск HiveServer2"

sudo systemctl restart hive-server2

wait_for_port "$HIVESERVER2_PORT" hive-server2 \
    || die "HiveServer2 не запустился."

# -------------------------------------------------------------------
# 10. Проверка подключения и метаданных
# -------------------------------------------------------------------

log "10. Проверка схемы Metastore"

sudo -u hadoop env "${SCHEMA_ENV[@]}" \
    "$HIVE_HOME/bin/schematool" -dbType postgres -info \
    || die "Не удалось проверить схему Metastore."

log "11. Проверка Beeline"

timeout 45 "$HIVE_HOME/bin/beeline" \
    -u "jdbc:hive2://${HIVE_HOST}:${HIVESERVER2_PORT}/default" \
    -e 'SHOW DATABASES;' \
    || {
        sudo journalctl -u hive-server2 -n 80 --no-pager
        die "Проверка Beeline завершилась ошибкой."
    }

log "Установка Hive завершена"

echo "Hive version: $HIVE_VERSION"
echo "Metastore:    thrift://127.0.0.1:$METASTORE_PORT"
echo "HiveServer2:  jdbc:hive2://${HIVE_HOST}:${HIVESERVER2_PORT}/default"

sudo systemctl --no-pager --full status \
    hive-metastore hive-server2
