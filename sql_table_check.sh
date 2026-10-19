#!/bin/bash
set -e

if [ "$(id -un)" != "hadoop" ]; then
    exec sudo -H -u hadoop bash -s -- "$@" < "$0"
fi

export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export HIVE_HOME=/opt/hive
export HIVE_CONF_DIR=/opt/hive/conf

JDBC_URL='jdbc:hive2://10.26.0.10:10000/default'

cd "$HOME"

cat > apartments_1.csv <<'EOF'
1,Moscow,10000000
2,Norilsk,5000000
5,Balachiha,9000000
EOF

cat > apartments_2.csv <<'EOF'
3,StPetersburg,15000000
4,Kolomna,7000000
EOF

DATA_DIR="$(pwd)"

"$HIVE_HOME/bin/beeline" \
  -u "$JDBC_URL" \
  -n hadoop \
  --force=false \
  -e "
CREATE DATABASE IF NOT EXISTS hw3_part2;

CREATE EXTERNAL TABLE IF NOT EXISTS hw3_part2.apartments (
    apartment_id INT,
    city STRING,
    price INT
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://team-26-nn:9000/user/hadoop/hw3_part2/apartments';

LOAD DATA LOCAL INPATH '$DATA_DIR/apartments_1.csv'
OVERWRITE INTO TABLE hw3_part2.apartments
PARTITION (dt='2026-10-01');

LOAD DATA LOCAL INPATH '$DATA_DIR/apartments_2.csv'
OVERWRITE INTO TABLE hw3_part2.apartments
PARTITION (dt='2026-10-02');

SHOW PARTITIONS hw3_part2.apartments;

SELECT * FROM hw3_part2.apartments LIMIT 10;
"
