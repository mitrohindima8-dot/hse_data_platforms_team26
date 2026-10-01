
#!/bin/bash

set -e

nodes=("team-26-nn" "team-26-00" "team-26-01")

while true; do
	read -s -p "Введите пароль для пользователя hadoop" password
        echo

        if [ -z "$password" ]; then
        	echo "Пустой пароль нельзя"
                continue
        fi

        read -s -p "Повторите пароль"  password_again
        echo

        if [ "$password" != "$password_again" ]; then 
        	echo "Пароли не совпадают"
                continue
        fi
	break
done

if id hadoop; then
	echo "Пользователь уже существует"
else
        sudo adduser --disabled-password --gecos "" hadoop
	sudo chpasswd <<< "hadoop:$password"
fi

for node in "${nodes[@]}"; do
	printf '%s\n' "$password" | ssh -i ~/.ssh/team_internal team@"$node" '
	read  password
	if id hadoop; then
		echo "Пользователь уже существует"
	else
		sudo adduser --disabled-password --gecos "" hadoop
		sudo chpasswd <<< "hadoop:$password"
		unset password
	fi
	'
done
unset password password_again

