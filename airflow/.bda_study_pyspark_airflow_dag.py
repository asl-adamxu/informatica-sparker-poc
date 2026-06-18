"""
PySpark 实战题 - Airflow DAG 版本
药品配送分析报告

此DAG可直接在Airflow中调度运行
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
    返回一个预配置了公共参数的 @task.pyspark 装饰器。
    用于在 YARN 上提交 Spark 作业并携带 Kerberos 凭证。
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
# DAG定义
# ============================================================================
@dag(
    dag_id='bda_study_pyspark_analysis',
    default_args=default_args,
    description='PySpark药品配送数据分析',
    schedule_interval=None,  # 手动触发或按需调度
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Shanghai'),
    catchup=False,
    tags=['adam'],
    params={
        'output_path': Param(
            default='/tmp/airflow_output/bda_analysis',
            type='string',
            description='输出数据的基础路径'
        ),
        'temp_data_path': Param(
            default='/tmp/airflow_output/raw_data',
            type='string',
            description='临时数据存储路径'
        )
    }
)
def bda_study_pyspark_analysis():
    """
    PySpark实战项目：药品配送数据分析DAG
    
    任务流程：
    1. 读取Oracle数据
    2. 数据探索（Task1）
    3. 机构分析（Task2）、患者分析（Task3）、时间分析（Task4）并行执行
    4. 数据质量检查（Task5）
    5. 保存结果（Task6）
    """
    
    # ========================================================================
    # 任务1：读取Oracle数据
    # ========================================================================
    @common_pyspark_task(task_id='load_oracle_data')
    def load_oracle_data(temp_data_path, **kw_args):
        """
        从Oracle读取数据表
        返回数据路径和行数统计
        """
        print("\n[Task] 正在读取Oracle数据...")

        spark = kw_args.get('spark')
        
        try:
            oracle_conn = BaseHook.get_connection("oracle_107")
            jdbc_schema = oracle_conn.extra_dejson.get("service_name") or oracle_conn.schema
            jdbc_url = f'jdbc:oracle:thin:@{oracle_conn.host}:{oracle_conn.port or 1521}/{jdbc_schema}'
            jdbc_user = oracle_conn.login
            jdbc_password = oracle_conn.password
            driver = 'oracle.jdbc.driver.OracleDriver'

            df = spark.read.format('jdbc') \
                .option('url', jdbc_url) \
                .option('dbtable', 'airflow.bda_study_source_table') \
                .option('user', jdbc_user) \
                .option('password', jdbc_password) \
                .option('driver', driver) \
                .option('numPartitions', '4') \
                .load()
            
            # 保存为临时Parquet
            df.coalesce(4).write.mode('overwrite').parquet(temp_data_path)
            
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
    # 任务2：数据探索
    # ========================================================================
    @common_pyspark_task(task_id='task_1_data_exploration')
    def task_1_data_exploration(data_info, temp_data_path, **kw_args):
        """
        任务1：基础数据清洗与探索
        """
        print("\n[Task 1] 数据探索开始...")

        spark = kw_args.get('spark')
        
        try:
            # 读取数据
            df = spark.read.parquet(temp_data_path)
            
            print(f"\n✓ 数据总行数：{data_info['total_rows']:,}")
            print(f"✓ 列数：{data_info['columns']}")
            
            # Schema
            print("\n✓ 数据Schema：")
            df.printSchema()
            
            # 空值检查
            print("\n✓ 关键列空值检查：")
            key_columns = ['patient_id', 'disp_qty', 'unitprice', 'itemcost', 'phs_inst_cd']
            
            null_check = df.select([
                count(when(col(c).isNull(), 1)).alias(f'{c}_null_count') 
                for c in key_columns
            ]).collect()[0]
            
            null_summary = {}
            for col_name in key_columns:
                null_count = null_check[f'{col_name}_null_count']
                null_pct = null_count / data_info['total_rows'] * 100
                null_summary[col_name] = {'count': null_count, 'percentage': null_pct}
                print(f"  - {col_name}: {null_count} 个NULL值 ({null_pct:.2f}%)")
            
            # 样本数据
            print("\n✓ 样本数据（前5行）：")
            df.limit(5).show(truncate=False)
            
            return {'status': 'success', 'null_summary': null_summary}
            
        except Exception as e:
            print(f"❌ 任务1失败: {str(e)}")
            raise
    
    
    # ========================================================================
    # 任务3：机构分析
    # ========================================================================
    @common_pyspark_task(task_id='task_2_institution_analysis')
    def task_2_institution_analysis(temp_data_path, output_path, **kw_args):
        """
        任务2：单机构配送分析
        """
        print("\n[Task 2] 机构分析开始...")

        spark = kw_args.get('spark')
        
        try:
            df = spark.read.parquet(temp_data_path)
            
            # 计算成本（处理null值）
            df_clean = df.withColumn(
                'total_cost', 
                when(col('itemcost').isNull() | col('disp_qty').isNull(), 0)
                .otherwise(col('itemcost') * col('disp_qty'))
            ).withColumn(
                'unitprice_filled',
                when(col('unitprice').isNull(), 0).otherwise(col('unitprice'))
            )
            
            # 按机构统计
            inst_analysis = df_clean.groupBy('phs_inst_cd').agg(
                count('*').alias('transaction_count'),
                spark_sum('disp_qty').alias('total_disp_qty'),
                spark_sum('total_cost').alias('total_cost'),
                avg('unitprice_filled').alias('avg_unitprice')
            ).orderBy(col('total_cost').desc()).limit(10)
            
            print("\n✓ Top 10 药房机构配送分析（按成本排序）：")
            inst_analysis.show(truncate=False)
            
            # 保存结果
            inst_output = f"{output_path}/institution_analysis"
            inst_analysis.coalesce(1).write.mode('overwrite').parquet(inst_output)
            print(f"✓ 机构分析结果已保存到：{inst_output}")
            
            return {
                'status': 'success',
                'output_path': inst_output,
                'record_count': inst_analysis.count()
            }
            
        except Exception as e:
            print(f"❌ 任务2失败: {str(e)}")
            raise
    
    
    # ========================================================================
    # 任务4：患者分析
    # ========================================================================
    @common_pyspark_task(task_id='task_3_patient_analysis')
    def task_3_patient_analysis(temp_data_path, output_path, **kw_args):
        """
        任务3：患者用药模式分析
        """
        print("\n[Task 3] 患者分析开始...")

        spark = kw_args.get('spark')
        
        try:
            df = spark.read.parquet(temp_data_path)
            
            # 患者分析
            patient_analysis = df.groupBy('patient_id').agg(
                count('*').alias('disp_frequency'),
                spark_sum('disp_qty').alias('total_disp_qty'),
                countDistinct('presc_no').alias('distinct_prescriptions'),
                countDistinct('item_no').alias('distinct_items')
            ).filter(col('disp_frequency') >= 3) \
             .orderBy(col('disp_frequency').desc()) \
             .limit(5)
            
            print("\n✓ Top 5 高频患者（配送次数>=3）：")
            patient_analysis.show(truncate=False)
            
            # 保存结果
            patient_output = f"{output_path}/patient_analysis"
            patient_analysis.coalesce(1).write.mode('overwrite').parquet(patient_output)
            print(f"✓ 患者分析结果已保存到：{patient_output}")
            
            return {
                'status': 'success',
                'output_path': patient_output,
                'record_count': patient_analysis.count()
            }
            
        except Exception as e:
            print(f"❌ 任务3失败: {str(e)}")
            raise
    
    
    # ========================================================================
    # 任务5：时间维度分析
    # ========================================================================
    @common_pyspark_task(task_id='task_4_temporal_analysis')
    def task_4_temporal_analysis(temp_data_path, output_path, **kw_args):
        """
        任务4：时间维度分析与移动平均
        """
        print("\n[Task 4] 时间维度分析开始...")

        spark = kw_args.get('spark')
        
        try:
            df = spark.read.parquet(temp_data_path)
            
            # 日聚合
            daily_stats = df.groupBy('disp_date_id').agg(
                spark_sum('disp_qty').alias('daily_total_qty'),
                spark_sum(col('itemcost') * col('disp_qty')).alias('daily_total_cost'),
                countDistinct('patient_id').alias('active_patients'),
                countDistinct('phs_inst_cd').alias('active_institutions'),
                count('*').alias('transaction_count')
            ).orderBy('disp_date_id')
            
            print("\n✓ 日均配送统计（前10天）：")
            daily_stats.limit(10).show(truncate=False)
            
            # 计算7日移动平均
            window_spec = Window.orderBy('disp_date_id').rangeBetween(-6, 0)
            
            moving_avg = daily_stats.withColumn(
                'ma7_qty', avg('daily_total_qty').over(window_spec)
            ).withColumn(
                'ma7_cost', avg('daily_total_cost').over(window_spec)
            ).select(
                'disp_date_id',
                'daily_total_qty',
                'ma7_qty',
                'daily_total_cost',
                'ma7_cost',
                'active_patients'
            ).orderBy('disp_date_id')
            
            print("\n✓ 7日移动平均分析（前15天）：")
            moving_avg.limit(15).show(truncate=False)
            
            # 保存结果
            daily_output = f"{output_path}/daily_stats"
            moving_avg.coalesce(1).write.mode('overwrite').parquet(daily_output)
            print(f"✓ 时间分析结果已保存到：{daily_output}")
            
            return {
                'status': 'success',
                'output_path': daily_output,
                'record_count': daily_stats.count()
            }
            
        except Exception as e:
            print(f"❌ 任务4失败: {str(e)}")
            raise
    
    
    # ========================================================================
    # 任务6：数据质量检查
    # ========================================================================
    @common_pyspark_task(task_id='task_5_data_quality_check')
    def task_5_data_quality_check(temp_data_path, output_path, **kw_args):
        """
        任务5：数据质量检查
        """
        print("\n[Task 5] 数据质量检查开始...")

        spark = kw_args.get('spark')
        
        try:
            df = spark.read.parquet(temp_data_path)
            
            # 1. 多剂次标志分布
            print("\n✓ 1. 多剂次标志分布：")
            multi_dose_dist = df.groupBy('multi_dose_ind').agg(
                count('*').alias('count'),
                (count('*') / df.count() * 100).alias('percentage')
            ).orderBy(col('count').desc())
            multi_dose_dist.show(truncate=False)
            
            # 2. 重复配送标志分布
            print("\n✓ 2. 重复配送标志分布：")
            refill_dist = df.groupBy('refill_ind').agg(
                count('*').alias('count'),
                (count('*') / df.count() * 100).alias('percentage')
            ).orderBy(col('count').desc())
            refill_dist.show(truncate=False)
            
            # 3. 异常值检测
            print("\n✓ 3. 异常值检测：")
            
            anomalies = df.select(
                col('*'),
                when(col('disp_qty') > 5000, 'HIGH_QTY').otherwise(None).alias('qty_anomaly'),
                when(col('unitprice') > 1000, 'HIGH_PRICE').otherwise(None).alias('price_anomaly')
            ).filter(
                (col('qty_anomaly').isNotNull()) | 
                (col('price_anomaly').isNotNull())
            )
            
            anomaly_count = anomalies.count()
            total_count = df.count()
            print(f"  - 异常记录总数：{anomaly_count} / {total_count} ({anomaly_count/total_count*100:.4f}%)")
            
            # 显示异常样本
            print("\n✓ 异常数据样本（前5条）：")
            anomalies.select(
                'patient_id', 'disp_qty', 'unitprice', 'itemcost',
                'qty_anomaly', 'price_anomaly'
            ).limit(5).show(truncate=False)
            
            # 保存异常数据
            anomaly_output = f"{output_path}/anomalies"
            anomalies.coalesce(1).write.mode('overwrite').parquet(anomaly_output)
            print(f"✓ 异常数据已保存到：{anomaly_output}")
            
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
    # 任务7：最终汇总与清理
    # ========================================================================
    @task(task_id='task_6_summary_and_cleanup', trigger_rule=TriggerRule.ALL_DONE)
    def task_6_summary_and_cleanup(
        exploration_result,
        inst_result,
        patient_result,
        temporal_result,
        quality_result,
        output_path
    ):
        """
        任务6：汇总结果并清理临时数据
        """
        print("\n[Task 6] 最终汇总与清理...")
        
        summary = {
            'execution_time': datetime.now().isoformat(),
            'tasks_status': {
                'data_exploration': exploration_result.get('status'),
                'institution_analysis': inst_result.get('status'),
                'patient_analysis': patient_result.get('status'),
                'temporal_analysis': temporal_result.get('status'),
                'quality_check': quality_result.get('status')
            },
            'output_locations': {
                'institution_analysis': inst_result.get('output_path'),
                'patient_analysis': patient_result.get('output_path'),
                'daily_stats': temporal_result.get('output_path'),
                'anomalies': quality_result.get('output_path')
            },
            'quality_metrics': {
                'anomaly_count': quality_result.get('anomaly_count'),
                'anomaly_percentage': quality_result.get('anomaly_percentage')
            }
        }
        
        print("\n✅ DAG执行完成！")
        print("\n汇总信息：")
        print(f"  - 机构分析记录数：{inst_result.get('record_count')}")
        print(f"  - 患者分析记录数：{patient_result.get('record_count')}")
        print(f"  - 时间维度记录数：{temporal_result.get('record_count')}")
        print(f"  - 异常检测记录数：{quality_result.get('anomaly_count')}")
        print(f"  - 异常比例：{quality_result.get('anomaly_percentage'):.4f}%")
        print(f"\n输出路径：{output_path}")
        
        return summary
    
    
    # ========================================================================
    # 定义任务依赖关系
    # ========================================================================
    
    # 获取参数
    output_path = "{{ params.output_path }}"
    temp_data_path = "{{ params.temp_data_path }}"
    
    # 任务流程
    data_info = load_oracle_data(temp_data_path)
    
    exploration = task_1_data_exploration(data_info, temp_data_path)
    
    # 三个分析任务并行执行
    inst = task_2_institution_analysis(temp_data_path, output_path)
    patient = task_3_patient_analysis(temp_data_path, output_path)
    temporal = task_4_temporal_analysis(temp_data_path, output_path)
    
    quality = task_5_data_quality_check(temp_data_path, output_path)
    
    summary = task_6_summary_and_cleanup(
        exploration, inst, patient, temporal, quality, output_path
    )
    
    # 定义依赖关系
    data_info >> exploration
    data_info >> [inst, patient, temporal]
    [inst, patient, temporal] >> quality >> summary


# 创建DAG实例
dag_instance = bda_study_pyspark_analysis()
