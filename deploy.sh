#!/bin/bash
set -e

KEY="$HOME/.ssh/team_internal"

echo "Запуск ResourceManager"
bash yarn_part1.sh

echo "Запуск NodeManager"
bash yarn_part2.sh

echo "Запуск MapReduce и HistoryServer"
ssh -i "$KEY" team@team-26-nn "sudo -u hadoop bash -s" < yarn_part3.sh

echo "Настройка NameNode UI"

if ! which nginx > /dev/null; then
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y nginx
fi

sudo tee /etc/nginx/sites-available/namenode > /dev/null <<'EOF'
server {
    listen 9870;

    location / {
        proxy_pass http://team-26-nn:9870;
        proxy_set_header Host $http_host;
    }
}
EOF

sudo ln -sf /etc/nginx/sites-available/namenode /etc/nginx/sites-enabled/namenode

sudo nginx -t
sudo systemctl enable nginx
sudo systemctl restart nginx

echo "Проверка HDFS"
ssh -i "$KEY" team@team-26-nn "sudo -u hadoop /opt/hadoop/bin/hdfs dfsadmin -report"

echo "Проверка YARN"
ssh -i "$KEY" team@team-26-nn "sudo -u hadoop /opt/hadoop/bin/yarn node -list"

echo "Проверка NameNode UI"
if curl -s -L -o /dev/null -w "%{http_code}" http://localhost:9870 | grep -q 200; then
    echo "NameNode UI работает"
else
    echo "Ошибка: NameNode UI не отвечает"
    exit 1
fi

echo "Кластер развернут"