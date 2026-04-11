```bash
conda activate valscope
```
# Mysql
## Activate mysql conda
```
conda activate sqlancer-env
```
## login mysql
```bash
/home/yaoruifei/miniconda3/envs/sqlancer-env/bin/mysql -u root -p -P 3307 -h 127.0.0.1
```

# MariaDB
## Activate mariadb conda
```
conda activate mariadb-env
```
## Login mariadb
```
mariadb --defaults-file=$HOME/mariadb/my.cnf -u root -p
```

```bash
python -u main.py > logs/run.log 2>&1

nohup python -u main.py

screen -S valscope
python -u main.py
# Ctrl+A 然后 D 挂起，SSH 断开也不影响
# 回来后 screen -r valscope 恢复
```