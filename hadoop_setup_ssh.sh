
#!/bin/bash

set -e

nodes=( "team-26-nn" "team-26-00" "team-26-01" )
#Изначально мы сидим в en
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
#мы сидим в en
if sudo test -f /home/hadoop/.ssh/id_ed25519; then 
	echo "SSH для hadoop в en уже создан"
else
	sudo -u hadoop mkdir -p /home/hadoop/.ssh
	sudo -u hadoop ssh-keygen -t ed25519 -N "" -f /home/hadoop/.ssh/id_ed25519
	sudo -u hadoop cat /home/hadoop/.ssh/id_ed25519.pub | sudo -u hadoop tee -a /home/hadoop/.ssh/authorized_keys > /dev/null
fi

for node in "${nodes[@]}"; do
	sudo cat /home/hadoop/.ssh/id_ed25519 | ssh -i ~/.ssh/team_internal team@"$node" '
		if ! sudo test -f /home/hadoop/.ssh/id_ed25519; then
			sudo -u hadoop mkdir -p /home/hadoop/.ssh
			sudo -u hadoop tee /home/hadoop/.ssh/id_ed25519 > /dev/null
			sudo chmod 600 /home/hadoop/.ssh/id_ed25519
		else
			cat > /dev/null
			echo " уже создан ssh priv и authorized_keys в hadoop"
		fi
'
	sudo cat /home/hadoop/.ssh/id_ed25519.pub | ssh -i ~/.ssh/team_internal team@"$node" '
		if ! sudo test -f /home/hadoop/.ssh/id_ed25519.pub; then
			sudo -u hadoop tee -a  /home/hadoop/.ssh/id_ed25519.pub /home/hadoop/.ssh/authorized_keys  > /dev/null
		else
			cat > /dev/null
			echo " уже создан ssh pub и authorized_keys в hadoop"
		fi
'
done

