#!/bin/bash
set -e

#запускаем на en от team, после hw4_run.sh

if [ "$(id -un)" != "hadoop" ]; then
    exec sudo -H -u hadoop bash -s -- "$@" < "$0"
fi

export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export HIVE_HOME=/opt/hive
export HIVE_CONF_DIR=/opt/hive/conf
cd "$HOME"

JDBC_URL='jdbc:hive2://10.26.0.10:10000/default'

#читаем через hive
#group by в hive падает на java 17
"$HIVE_HOME/bin/beeline" -u "$JDBC_URL" -n hadoop -e "
SHOW PARTITIONS hw4.apartments_stats;
SELECT * FROM hw4.apartments_stats WHERE month = '2026-03' LIMIT 10;
" 2>/dev/null

#выгрузка из hive и hdfs
"$HIVE_HOME/bin/beeline" -u "$JDBC_URL" -n hadoop --silent=true --showHeader=false --outputformat=tsv2 \
    -e "SELECT month, city, rooms, deals, avg_price FROM hw4.apartments_stats;" 2>/dev/null > hw4_table.tsv
"$HADOOP_HOME/bin/hdfs" dfs -cat /user/hadoop/hw4/input/apartments.csv > hw4_input.csv
partitions=$("$HADOOP_HOME/bin/hdfs" dfs -ls /user/hadoop/hw4/apartments_stats | grep -c "month=" || true)

#сверка с csv
python3 - hw4_input.csv hw4_table.tsv "$partitions" <<'EOF'
import csv
import sys

expected = {}
with open(sys.argv[1]) as f:
    for r in csv.DictReader(f):
        if not r["price"] or float(r["area"]) <= 0:
            continue
        key = (r["sale_date"][:7], r["city"], r["rooms"])
        expected.setdefault(key, []).append(int(r["price"]))

got = {}
with open(sys.argv[2]) as f:
    for line in f:
        p = line.rstrip("\n").split("\t")
        if len(p) == 5:
            got[(p[0], p[1], p[2])] = (int(p[3]), int(p[4]))

errors = 0
for key, prices in expected.items():
    if key not in got:
        errors += 1
        continue
    deals, avg_price = got[key]
    if deals != len(prices) or abs(avg_price - sum(prices) / len(prices)) > 1:
        errors += 1
if len(got) != len(expected):
    errors += 1

months = len({k[0] for k in expected})
partitions = int(sys.argv[3])
total = sum(len(v) for v in expected.values())

print("сделок после очистки в csv:", total, "в таблице hive:", sum(d for d, _ in got.values()))
print("групп в csv:", len(expected), "в таблице hive:", len(got))
print("месяцев в csv:", months, "партиций в hdfs:", partitions)

if errors or partitions != months:
    print("Ошибка: данные в таблице не совпадают с исходными")
    sys.exit(1)
print("Проверка пройдена: hive читает таблицу, данные и партиции совпадают")
EOF

rm -f hw4_table.tsv hw4_input.csv
