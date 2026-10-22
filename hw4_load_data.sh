#!/bin/bash
set -e

#запускаем на en от team

ROWS=5000
TMP_CSV=/tmp/hw4_apartments.csv
HDFS_DIR=/user/hadoop/hw4/input

python3 - "$ROWS" "$TMP_CSV" <<'EOF'
import csv
import random
import sys
from datetime import date, timedelta

rows = int(sys.argv[1])
random.seed(26)
#цена за м2
cities = {"Moscow": 300000, "Saint Petersburg": 220000, "Kazan": 150000, "Novosibirsk": 120000, "Yekaterinburg": 130000}
start = date(2026, 1, 1)

with open(sys.argv[2], "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["id", "city", "rooms", "area", "price", "sale_date"])
    for i in range(1, rows + 1):
        city = random.choice(list(cities))
        rooms = random.randint(1, 4)
        area = round(random.uniform(18, 25) * rooms + random.uniform(5, 15), 1)
        price = int(area * cities[city] * random.uniform(0.8, 1.3))
        day = start + timedelta(days=random.randint(0, 272))
        #битые строки
        if i % 100 == 0:
            price = ""
        if i % 250 == 0:
            area = -1
        w.writerow([i, city, rooms, area, price, day.isoformat()])
EOF

chmod 644 "$TMP_CSV"
sudo -u hadoop /opt/hadoop/bin/hdfs dfs -mkdir -p "$HDFS_DIR"
sudo -u hadoop /opt/hadoop/bin/hdfs dfs -put -f "$TMP_CSV" "$HDFS_DIR/apartments.csv"
rm -f "$TMP_CSV"

sudo -u hadoop /opt/hadoop/bin/hdfs dfs -ls "$HDFS_DIR"
echo "Данные загружены в $HDFS_DIR: $ROWS строк"
