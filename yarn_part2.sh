#!/bin/bash
set -e

#запускаем на en от team, после yarn_part1.sh

KEY="$HOME/.ssh/team_internal"
NODES=( "team-26-nn" "team-26-00" "team-26-01" )

#nodemanager на всех нодах
for node in "${NODES[@]}"; do
    echo "$node: настраиваю nodemanager"
    ssh -i "$KEY" team@"$node" "sudo -u hadoop bash -s" <<'EOF'
set -e
HADOOP_HOME=/opt/hadoop
YARN_CONFIG="$HADOOP_HOME/etc/hadoop/yarn-site.xml"

add_property() {
    if ! grep -q "<name>$1</name>" "$YARN_CONFIG"; then
        sed -i "/<\/configuration>/i\\
    <property>\\
        <name>$1</name>\\
        <value>$2</value>\\
    </property>" "$YARN_CONFIG"
    fi
}

add_property yarn.resourcemanager.hostname team-26-nn
add_property yarn.nodemanager.aux-services mapreduce_shuffle
add_property yarn.nodemanager.env-whitelist JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME
#на нодах 3гб и 2 ядра
add_property yarn.nodemanager.resource.memory-mb 2048
add_property yarn.nodemanager.resource.cpu-vcores 2
add_property yarn.nodemanager.vmem-check-enabled false
add_property yarn.nodemanager.webapp.address 0.0.0.0:8042

"$HADOOP_HOME/bin/yarn" --daemon stop nodemanager || true
"$HADOOP_HOME/bin/yarn" --daemon start nodemanager
EOF
done

#проверка
running=0
for i in $(seq 1 12); do
    sleep 5
    running=$(ssh -i "$KEY" team@team-26-nn "sudo -u hadoop /opt/hadoop/bin/yarn node -list 2>/dev/null" | grep -c RUNNING || true)
    if [ "$running" -ge 3 ]; then
        break
    fi
done

if [ "$running" -ge 3 ]; then
    echo "NodeManager работают: $running из 3"
else
    echo "Ошибка: работает NodeManager $running из 3"
    exit 1
fi

#nginx на en: 8042 nn, 8043 00, 8044 01
if ! which nginx > /dev/null; then
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y nginx
fi

sudo tee /etc/nginx/sites-available/nodemanager > /dev/null <<'EOF'
server {
    listen 8042;
    location / {
        proxy_pass http://team-26-nn:8042;
        proxy_set_header Host $http_host;
    }
}

server {
    listen 8043;
    location / {
        proxy_pass http://team-26-00:8042;
        proxy_set_header Host $http_host;
    }
}

server {
    listen 8044;
    location / {
        proxy_pass http://team-26-01:8042;
        proxy_set_header Host $http_host;
    }
}
EOF

sudo ln -sf /etc/nginx/sites-available/nodemanager /etc/nginx/sites-enabled/nodemanager
sudo nginx -t
sudo systemctl enable nginx
sudo systemctl restart nginx

for port in 8042 8043 8044; do
    if curl -s -L -o /dev/null -w "%{http_code}" http://localhost:$port/node | grep -q 200; then
        echo "Веб-интерфейс на порту $port работает"
    else
        echo "Ошибка: веб-интерфейс на порту $port не отвечает"
        exit 1
    fi
done
