from datetime import datetime
from airflow import DAG
from airflow.decorators import task

DAG_ID = 'adam_simple_function_dag'
log_file = f'~/.airflow_logs/{DAG_ID}_{datetime.now().strftime("%Y%m%d%H%M%S")}.log'

# ────────────────────────────────────────────────────────────────
# ① 共享业务函数 —— `function1`
# ────────────────────────────────────────────────────────────────
def function1(**kw):
    """
    简单的业务逻辑示例（不使用 Spark）
    """
    data = [("Alice", 1), ("Bob", 2), ("Catherine", 3)]
    print("模拟 DataFrame 数据:")
    for name, value in data:
        print(f"  {name}: {value}")
    return data

# ────────────────────────────────────────────────────────────────
# ②  stage1 —— 调用 `function1`
# ────────────────────────────────────────────────────────────────
@task
def stage1(**kw):
    # 调用公共函数
    data = function1(**kw)

    # 计算数量
    count = len(data)
    print(f"[stage1] 数据行数 = {count}")
    
    # 写入日志文件
    with open(log_file, 'a') as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - [stage1] 数据行数 = {count}\n")
    
    return count

# ────────────────────────────────────────────────────────────────
# ③  stage2 —— 同样调用 `function1`
# ────────────────────────────────────────────────────────────────
@task
def stage2(**kw):
    data = function1(**kw)

    # 计算总和
    total = sum(value for _, value in data)
    print(f"[stage2] 总和 = {total}")
    
    # 写入日志文件
    with open(log_file, 'a') as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - [stage2] 总和 = {total}\n")
    
    return total

# ────────────────────────────────────────────────────────────────
# ④ DAG 定义
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    tags=["adam"],
    start_date=datetime(2026, 4, 16, 5, 6),
    schedule_interval="@once",          # 仅运行一次
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
    }
) as dag:
    # 调用 stage1 → stage2
    t1 = stage1()
    t2 = stage2()
    t1 >> t2        # t2 暂停等待 t1 完成

