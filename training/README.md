# PySpark 实战培训

- training_assignment.md          — 题目
- training_knowledge.md           — 相关知识点
- bda_study_pyspark_training.py   — 参考模板

# Airflow 部署运行

## 步骤

```bash
# 1. 复制模版到 Airflow dags 个人目录
cp pyspark_starter_template.py $AIRFLOW_HOME/dags/

# 2. 验证语法
airflow dags validate pyspark_starter_template.py

# 3. 手动触发运行
airflow dags trigger bda_study_pyspark_training

# 4. 查看运行状态
airflow dags list-runs --dag-id bda_study_pyspark_training
```


## 监控

`http://192.168.55.104:7070/` 

```bash
airflow tasks logs bda_study_pyspark_training <task_id> <date>
airflow tasks list bda_study_pyspark_training
```
