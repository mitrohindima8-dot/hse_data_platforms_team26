#!/usr/bin/env bash
# ВАЖНО - нужно находиться на team-26-nn:


set -euo pipefail

HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_HOME}/etc/hadoop}"
MAPRED_CONF="${CONF_DIR}/mapred-site.xml"
EXAMPLES_JAR="${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar"
HISTORY_URL="http://127.0.0.1:19888/jobhistory/"

echo "== Hadoop task 3 =="

if [[ "$(id -un)" != "hadoop" ]]; then
  echo "NOTE: current user is '$(id -un)', not 'hadoop'."
  echo "Run as the Hadoop service user if that is how your cluster is configured."
fi

if [[ ! -d "$CONF_DIR" ]]; then
  echo "ERROR: Hadoop config directory not found: $CONF_DIR" >&2
  exit 1
fi

if [[ ! -f "$EXAMPLES_JAR" ]]; then
  echo "ERROR: examples JAR not found: $EXAMPLES_JAR" >&2
  echo "Check Hadoop version/path before continuing." >&2
  exit 1
fi


if [[ -f "$MAPRED_CONF" ]]; then
  BACKUP="${MAPRED_CONF}.backup.$(date +%Y%m%d-%H%M%S)"
  cp -p "$MAPRED_CONF" "$BACKUP"
  echo "Backup created: $BACKUP"
else
  echo "No existing mapred-site.xml; creating it."
fi

cat > "$MAPRED_CONF" <<'XML'
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*,$HADOOP_COMMON_HOME/share/hadoop/common/*,$HADOOP_COMMON_HOME/share/hadoop/common/lib/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*,$HADOOP_YARN_HOME/share/hadoop/yarn/*,$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*</value>
    </property>
</configuration>
XML

echo
echo "== mapred-site.xml written =="
cat "$MAPRED_CONF"

echo
echo "== Starting JobHistoryServer =="
if ! jps | grep -q JobHistoryServer; then
  "$HADOOP_HOME/bin/mapred" --daemon start historyserver
fi
sleep 2

if command -v jps >/dev/null 2>&1; then
  jps
fi

echo
echo "== Checking HistoryServer HTTP endpoint =="
HTTP_CODE="$(curl -sS -L --max-time 15 -o /dev/null -w '%{http_code}' "$HISTORY_URL" || true)"
echo "HTTP status: $HTTP_CODE"
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "ERROR: HistoryServer did not return HTTP 200 at $HISTORY_URL" >&2
  echo "Check the process/logs and ensure port 19888 is reachable locally." >&2
  exit 1
fi

echo
echo "== Running MapReduce Pi test =="
"$HADOOP_HOME/bin/hadoop" jar "$EXAMPLES_JAR" pi 2 10

echo
echo "== Final HistoryServer check =="
curl -sS -L --max-time 15 "$HISTORY_URL" | grep -Eo 'job_[0-9]+_[0-9]+' | sort -u | tail -n 10 || true

echo
echo "Finished"
echo "http://localhost:19888/jobhistory"
