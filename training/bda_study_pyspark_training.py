"""
Airflow DAG 实战模板
药品配送分析报告
此模版可直接在Airflow中调度运行，需填空完成各个任务函数。
"""

import findspark
findspark.init("/opt/cloudera/parcels/SPARK3/lib/spark3")

from airflow.decorators import dag, task
from airflow import DAG
from airflow.models.param import Param
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule
import pendulum
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, countDistinct, when
)
from pyspark.sql.window import Window
import sys


def common_pyspark_task(**override_kwargs):
    """
    ★ 已封装好的公共Spark任务装饰器 ★
    - 自动配置 Kerberos keytab/principal
    - 提交到 YARN (spark3_on_yarn)
    - 无需修改此函数
    """
    base_config = {
        "conn_id": "spark3_on_yarn",
        "config_kwargs": {
            "spark.kerberos.keytab": "/appl/hadoop/cdp/keytabs/etl_user.keytab",
            "spark.kerberos.principal": "etl_user@AIDA.COM",
            "spark.executorEnv.PYTHONPATH": "/opt/cloudera/parcels/SPARK3/lib/spark3/python:/opt/cloudera/parcels/SPARK3/lib/spark3/python/lib/py4j-0.10.9.7-src.zip",
        },
    }

    if "config_kwargs" in override_kwargs:
        base_config["config_kwargs"].update(override_kwargs.pop("config_kwargs"))

    base_config.update(override_kwargs)

    return task.pyspark(**base_config)


# ============================================================================
# 默认DAG参数
# ============================================================================
default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}


# ============================================================================
# DAG定义 — 请关注 @common_pyspark_task 修饰的函数中的填空
# ============================================================================
@dag(
    dag_id='bda_study_pyspark_training',
    default_args=default_args,
    description='[培训] PySpark药品配送数据分析',
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Shanghai'),
    catchup=False,
    tags=['training'],
    params={
        'output_path': Param(
            default='/tmp/airflow_output/bda_training',
            type='string',
            description='输出数据的基础路径'
        ),
    }
)
def bda_study_pyspark_training():
    """
    PySpark实战培训DAG

    任务流程（需填空的任务）：
    0. load_oracle_data   - 从Oracle读取数据
    1. task_1_exploration - 数据探索
    2. task_2_institution - 机构分析
    3. task_3_patient     - 患者分析
    4. task_4_temporal    - 时间维度分析
    5. task_5_quality     - 数据质量检查
    6. task_6_summary     - 汇总输出
    """

    # ========================================================================
    # 任务1：从Oracle读取数据 ★ 填空
    # ========================================================================
    @common_pyspark_task(task_id='load_oracle_data')
    def load_oracle_data(**kw_args):
        """
        ★ 任务 ★
        从Oracle读取 bda_study_source_table 表，保存为Parquet临时文件

        提示：
        1. spark = kw_args.get('spark')  ← Airflow注入的SparkSession
        2. 使用 BaseHook.get_connection("oracle_107") 获取连接
        3. spark.read.format('jdbc') 读取数据
        4. df.write.mode('overwrite').parquet(path) 保存
        5. 返回 {'data_path': path, 'total_rows': count, 'columns': len}

        返回：dict（通过XCom传递给下游任务）
        """
        print("\n[Task] 正在读取Oracle数据...")

        # ★ 从 kw_args 获取 Airflow 注入的 SparkSession
        spark = kw_args.get('spark')

        try:
            # ===== 请在此处填空 =====
            # 1. 获取Oracle连接
            oracle_conn = ...

            # 2. 构建JDBC URL
            jdbc_url = ...

            # 3. 读取数据
            df = ...

            # 4. 保存为临时Parquet（供后续任务读取）
            temp_data_path = '/tmp/airflow_output/raw_data'
            ...
            # ===== 填空结束 =====

            total_rows = df.count()
            print(f"✓ 成功读取 {total_rows:,} 行数据")
            print(f"✓ 数据已保存到临时路径：{temp_data_path}")

            return {
                'data_path': temp_data_path,
                'total_rows': total_rows,
                'columns': len(df.columns)
            }

        except Exception as e:
            print(f"❌ 读取数据失败: {str(e)}")
            raise


    # ========================================================================
    # 任务2：数据探索 ★ 填空
    # ========================================================================
    @common_pyspark_task(task_id='task_1_exploration')
    def task_1_exploration(data_info, **kw_args):
        """
        ★ 任务 ★
        对已读取的数据进行探索性分析

        要求：
        1. 打印总行数、列数
        2. 显示DataFrame的Schema
        3. 检查关键列 ['patient_id', 'disp_qty', 'unitprice', 'itemcost'] 的空值
        4. 显示前5行样本数据

        提示：
        - spark = kw_args.get('spark')
        - df = spark.read.parquet(data_info['data_path'])
        - 使用 count(when(col(c).isNull(), 1)) 统计空值

        返回：dict (status, null_summary)
        """
        print("\n[Task 1] 数据探索开始...")

        spark = kw_args.get('spark')

        try:
            # ===== 请在此处填空 =====
            # 1. 从Parquet读取数据
            df = ...

            # 2. 打印总行数
            total_rows = ...
            print(f"\n✓ 数据总行数：{total_rows:,}")
            print(f"✓ 列数：{data_info['columns']}")

            # 3. 显示Schema
            print("\n✓ 数据Schema：")
            ...

            # 4. 检查空值
            print("\n✓ 关键列空值检查：")
            key_columns = ['patient_id', 'disp_qty', 'unitprice', 'itemcost', 'phs_inst_cd']
            # 提示：df.select([count(when(col(c).isNull(), 1)).alias(...) for c in key_columns])
            null_check = ...
            # 用 null_check.collect()[0] 获取结果

            # 5. 显示样本
            print("\n✓ 样本数据（前5行）：")
            ...
            # ===== 填空结束 =====

            return {'status': 'success', 'null_summary': '...'}

        except Exception as e:
            print(f"❌ 任务1失败: {str(e)}")
            raise


    # ========================================================================
    # 任务3：机构分析 ★ 填空
    # ========================================================================
    @common_pyspark_task(task_id='task_2_institution')
    def task_2_institution(data_info, output_path, **kw_args):
        """
        ★ 任务 ★
        按 phs_inst_cd（药房机构）分析配送数据

        要求：
        1. 计算总成本 = itemcost * disp_qty（处理null值）
        2. 按 phs_inst_cd 分组，统计：
           - 交易数量 (count(*))
           - 总配送数量 (sum(disp_qty))
           - 总成本 (sum(total_cost))
           - 平均单价 (avg(unitprice))
        3. 按总成本降序排列，取Top 10
        4. 保存结果到 {output_path}/institution_analysis

        提示：
        - withColumn 新建列
        - when(col().isNull(), 0).otherwise(...) 处理null值
        - groupBy('col').agg(...) 分组聚合
        - orderBy(col('col').desc()).limit(10) 排序取Top

        返回：dict (status, output_path, record_count)
        """
        print("\n[Task 2] 机构分析开始...")

        spark = kw_args.get('spark')

        try:
            # ===== 请在此处填空 =====
            df = spark.read.parquet(data_info['data_path'])

            # 1. 处理null值，计算总成本（新增 total_cost 列）
            df_clean = df.withColumn(
                'total_cost',
                ...  # when / otherwise
            )

            # 2. 按机构分组聚合
            inst_analysis = df_clean.groupBy(...).agg(
                count('*').alias('transaction_count'),
                ...,
                ...,
                ...
            ).orderBy(...).limit(10)

            print("\n✓ Top 10 药房机构配送分析（按成本排序）：")
            inst_analysis.show(truncate=False)

            # 3. 保存结果
            inst_output = f"{output_path}/institution_analysis"
            ...
            # ===== 填空结束 =====

            return {
                'status': 'success',
                'output_path': inst_output,
                'record_count': inst_analysis.count()
            }

        except Exception as e:
            print(f"❌ 任务2失败: {str(e)}")
            raise


    # ========================================================================
    # 任务4：患者分析 ★ 填空
    # ========================================================================
    @common_pyspark_task(task_id='task_3_patient')
    def task_3_patient(data_info, output_path, **kw_args):
        """
        ★ 任务 ★
        按 patient_id 分析患者用药模式

        要求：
        1. 按 patient_id 分组统计：
           - 配送频次 (count(*))
           - 总配送数量 (sum(disp_qty))
           - 处方数 (countDistinct(presc_no))
           - 药品数 (countDistinct(item_no))
        2. 筛选配送频次 >= 3 的患者
        3. 按频次降序，取Top 5
        4. 保存结果到 {output_path}/patient_analysis

        提示：
        - filter(col('col') >= 3) 筛选条件

        返回：dict (status, output_path, record_count)
        """
        print("\n[Task 3] 患者分析开始...")

        spark = kw_args.get('spark')

        try:
            # ===== 请在此处填空 =====
            df = spark.read.parquet(data_info['data_path'])

            patient_analysis = df.groupBy(...).agg(
                count('*').alias('disp_frequency'),
                ...,
                ...,
                ...
            ).filter(...).orderBy(...).limit(5)

            print("\n✓ Top 5 高频患者（配送次数>=3）：")
            patient_analysis.show(truncate=False)

            # 保存结果
            patient_output = f"{output_path}/patient_analysis"
            ...
            # ===== 填空结束 =====

            return {
                'status': 'success',
                'output_path': patient_output,
                'record_count': patient_analysis.count()
            }

        except Exception as e:
            print(f"❌ 任务3失败: {str(e)}")
            raise


    # ========================================================================
    # 任务5：时间维度分析 ★ 填空
    # ========================================================================
    @common_pyspark_task(task_id='task_4_temporal')
    def task_4_temporal(data_info, output_path, **kw_args):
        """
        ★ 任务 ★
        按 disp_date_id 分析时间维度趋势

        要求：
        1. 按 disp_date_id 分组统计每日：
           - 日配送总量 (sum(disp_qty))
           - 日配送总成本 (sum(itemcost * disp_qty))
           - 活跃患者数 (countDistinct(patient_id))
           - 活跃机构数 (countDistinct(phs_inst_cd))
        2. 计算7日移动平均配送量
        3. 显示前15天结果
        4. 保存结果到 {output_path}/daily_stats

        提示：
        - Window.orderBy('disp_date_id').rangeBetween(-6, 0)

        返回：dict (status, output_path, record_count)
        """
        print("\n[Task 4] 时间维度分析开始...")

        spark = kw_args.get('spark')

        try:
            # ===== 请在此处填空 =====
            df = spark.read.parquet(data_info['data_path'])

            # 1. 日聚合
            daily_stats = df.groupBy(...).agg(
                ...,
                ...,
                countDistinct('patient_id').alias('active_patients'),
                ...
            ).orderBy('disp_date_id')

            # 2. 计算7日移动平均
            window_spec = ...
            moving_avg = daily_stats.withColumn(
                'ma7_qty',
                ...
            ).select(
                'disp_date_id',
                'daily_total_qty',
                'ma7_qty'
            )

            print("\n✓ 7日移动平均分析（前15天）：")
            moving_avg.limit(15).show(truncate=False)

            # 保存结果
            daily_output = f"{output_path}/daily_stats"
            ...
            # ===== 填空结束 =====

            return {
                'status': 'success',
                'output_path': daily_output,
                'record_count': daily_stats.count()
            }

        except Exception as e:
            print(f"❌ 任务4失败: {str(e)}")
            raise


    # ========================================================================
    # 任务6：数据质量检查 ★ 填空
    # ========================================================================
    @common_pyspark_task(task_id='task_5_quality')
    def task_5_quality(data_info, output_path, **kw_args):
        """
        ★ 任务 ★
        数据质量检查

        要求：
        1. 统计 multi_dose_ind 字段值的分布
        2. 统计 refill_ind 字段值的分布
        3. 检测异常值：
           - disp_qty > 5000 (数量异常)
           - unitprice > 1000 (单价异常)
           - itemcost * disp_qty > 100000 (成本异常)
        4. 显示异常类型分布
        5. 保存异常数据到 {output_path}/anomalies

        提示：
        - groupBy('col').agg(count('*'), ...) 统计分布
        - when(condition, tag).otherwise(None).alias('col') 打标签
        - filter( condition1 | condition2 | ...) 筛选异常

        返回：dict (status, output_path, anomaly_count, anomaly_percentage)
        """
        print("\n[Task 5] 数据质量检查开始...")

        spark = kw_args.get('spark')

        try:
            # ===== 请在此处填空 =====
            df = spark.read.parquet(data_info['data_path'])

            # 1. 多剂次标志分布
            print("\n✓ 1. 多剂次标志分布：")
            multi_dose_dist = ...
            multi_dose_dist.show(truncate=False)

            # 2. 重复配送标志分布
            print("\n✓ 2. 重复配送标志分布：")
            refill_dist = ...
            refill_dist.show(truncate=False)

            # 3. 异常值检测
            print("\n✓ 3. 异常值检测：")
            anomalies = df.select(
                col('*'),
                when(col('disp_qty') > 5000, 'HIGH_QTY').otherwise(None).alias('qty_anomaly'),
                ...,
                ...
            ).filter(
                (col('qty_anomaly').isNotNull()) | ...
            )

            anomaly_count = anomalies.count()
            total_count = df.count()
            print(f"  - 异常记录总数：{anomaly_count} / {total_count}")

            # 4. 保存异常数据
            anomaly_output = f"{output_path}/anomalies"
            ...
            # ===== 填空结束 =====

            return {
                'status': 'success',
                'output_path': anomaly_output,
                'anomaly_count': anomaly_count,
                'anomaly_percentage': anomaly_count / total_count * 100
            }

        except Exception as e:
            print(f"❌ 任务5失败: {str(e)}")
            raise


    # ========================================================================
    # 任务7：汇总输出（无需填空，可直接使用）
    # ========================================================================
    @task(task_id='task_6_summary', trigger_rule=TriggerRule.ALL_DONE)
    def task_6_summary(
        exploration_result,
        inst_result,
        patient_result,
        temporal_result,
        quality_result,
        output_path
    ):
        """
        汇总所有任务结果并输出报告（无需填空）
        """
        print("\n[Task 6] 最终汇总...")

        summary = {
            'execution_time': datetime.now().isoformat(),
            'tasks_status': {
                'exploration': exploration_result.get('status'),
                'institution': inst_result.get('status'),
                'patient': patient_result.get('status'),
                'temporal': temporal_result.get('status'),
                'quality': quality_result.get('status')
            },
            'output_locations': {
                'institution': inst_result.get('output_path'),
                'patient': patient_result.get('output_path'),
                'daily_stats': temporal_result.get('output_path'),
                'anomalies': quality_result.get('output_path')
            }
        }

        print("\n✅ DAG执行完成！")
        print(f"\n输出路径：{output_path}")
        print(f"任务状态：{summary['tasks_status']}")

        return summary


    # ========================================================================
    # ★ 任务依赖关系 — 需理解 DAG 的任务编排 ★
    # ========================================================================

    output_path = "{{ params.output_path }}"

    data_info = load_oracle_data()                         # 任务1：读取数据
    exploration = task_1_exploration(data_info)            # 任务2：数据探索

    # 任务3、4、5 可并行执行（无依赖关系）
    inst = task_2_institution(data_info, output_path)      # 任务3：机构分析
    patient = task_3_patient(data_info, output_path)       # 任务4：患者分析
    temporal = task_4_temporal(data_info, output_path)     # 任务5：时间分析

    quality = task_5_quality(data_info, output_path)       # 任务6：质量检查

    summary = task_6_summary(                              # 任务7：汇总
        exploration, inst, patient, temporal, quality, output_path
    )

    # 依赖关系：
    data_info >> exploration                               # 先探索
    data_info >> [inst, patient, temporal]                 # 再并行分析
    [inst, patient, temporal] >> quality >> summary        # 最后质量检查→汇总


# ============================================================================
# 创建DAG实例（Airflow自动加载）
# ============================================================================
dag_instance = bda_study_pyspark_training()
