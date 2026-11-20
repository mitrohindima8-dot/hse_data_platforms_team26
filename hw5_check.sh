#!/bin/bash
set -euo pipefail

export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export HIVE_HOME=/opt/hive

JDBC_URL='jdbc:hive2://10.26.0.10:10000/default'
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

"$HADOOP_HOME/bin/hdfs" dfs -cat /user/hadoop/hw4/input/apartments.csv > "$tmp_dir/input.csv"
"$HADOOP_HOME/bin/hdfs" dfs -ls /user/team/team_prefect/apartments_stats > "$tmp_dir/hdfs.txt"

"$HIVE_HOME/bin/beeline" -u "$JDBC_URL" -n team \
  --silent=true --showHeader=false --outputformat=tsv2 \
  -e 'SELECT month, city, rooms, deals, avg_price FROM team_prefect.apartments_stats;' \
  > "$tmp_dir/table.tsv"

"$HIVE_HOME/bin/beeline" -u "$JDBC_URL" -n team \
  --silent=true --showHeader=false --outputformat=tsv2 \
  -e 'SHOW PARTITIONS team_prefect.apartments_stats;' > "$tmp_dir/partitions.txt"
  
python3 - "$tmp_dir" <<'EOF'
import csv
import math
import sys
from datetime import datetime

folder = sys.argv[1]
expected = {}
seen = set()

with open(folder + "/input.csv") as f:
    for r in csv.DictReader(f):
        try:
            ident, rooms = int(r["id"]), int(r["rooms"])
            area, price = float(r["area"]), int(r["price"])
            day = datetime.strptime(r["sale_date"], "%Y-%m-%d")
        except (ValueError, TypeError):
            continue
        if not (area > 0 and price > 0 and rooms > 0) or ident in seen:
            continue
        seen.add(ident)
        city = r["city"].strip() if r["city"] else "unknown"
        key = (day.strftime("%Y-%m"), city, rooms)
        expected.setdefault(key, []).append(price)

got = {}
with open(folder + "/table.tsv") as f:
    for p in csv.reader(f, delimiter="\t"):
        if not p:
            continue
        if len(p) != 5:
            sys.exit("Ошибка: не удалось разобрать вывод Hive")
        key = (p[0], p[1], int(p[2]))
        if key in got:
            sys.exit("Ошибка: в таблице повторяются группы")
        got[key] = (int(p[3]), int(p[4]))

errors = 0
for key, prices in expected.items():
    avg_price = math.floor(sum(prices) / len(prices) + 0.5)
    if got.get(key) != (len(prices), avg_price):
        errors += 1
if expected.keys() != got.keys() or not expected:
    errors += 1

months = {"month=" + key[0] for key in expected}
with open(folder + "/partitions.txt") as f:
    hive_parts = set(f.read().split())
hdfs_parts = set()
with open(folder + "/hdfs.txt") as f:
    for line in f:
        if not line.strip():
            continue
        name = line.split()[-1].rsplit("/", 1)[-1]
        if name.startswith("month="):
            hdfs_parts.add(name)

print("Сделок после очистки в CSV:", len(seen), "; в Hive:", sum(d for d, _ in got.values()))
print("Групп в CSV:", len(expected), "; в Hive:", len(got))
print("Партиций: ожидается", len(months), "; Hive", len(hive_parts), "; HDFS", len(hdfs_parts))
if errors or months != hive_parts or months != hdfs_parts:
    print("Ошибка: данные или партиции не совпадают")
    sys.exit(1)
print("Проверка пройдена: Hive читает таблицу, данные и партиции совпадают")
EOF
