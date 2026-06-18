git init
git config --global user.email "adamxu@aslcn.com.cn"
git config --global user.name "adamxu"

# 从上游仓库同步指定子目录example
cd /var/lib/airflow/dags/jack

# 1. 添加上游远程源（只需一次）
git remote add upstream /var/lib/airflow/dags/adam
git sparse-checkout init --cone
git sparse-checkout set informatica/PowerCenter_workflows # 设置只检出子目录

# 拉取上游最新代码，保留本地变更
git fetch upstream master
git merge upstream/master

# 查看变更
git diff upstream/master -- informatica/PowerCenter_workflows/

# 或用checkout直接覆盖
git checkout upstream/master 

