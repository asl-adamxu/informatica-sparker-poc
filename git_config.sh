git init
git config --global user.email "adamxu@aslcn.com.cn"
git config --global user.name "adamxu"

# 从上游仓库同步指定子目录example
cd /var/lib/airflow/dags/jack

# 添加上游远程源（只需一次）
git remote add upstream /var/lib/airflow/dags/adam

# 拉取上游最新代码
git fetch upstream master

# 直接从上游master检出子目录
git checkout upstream/master -- informatica/PowerCenter_workflows/

# 提交变更
git add informatica/PowerCenter_workflows/
git commit -m "sync: update PowerCenter_workflows from adam master"
