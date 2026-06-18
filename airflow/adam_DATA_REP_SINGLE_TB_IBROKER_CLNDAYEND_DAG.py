import findspark
findspark.init("/opt/cloudera/parcels/SPARK3/lib/spark3")
import re
from airflow.decorators import task, task_group
from airflow import DAG
from airflow.models.param import Param
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule
import airflow
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import expr, col, lit, count
import pendulum

def common_pyspark_task(**override_kwargs):
    """
    返回一个预配置了公共参数的 @task.pyspark 装饰器。
    override_kwargs 可用于覆盖或补充默认配置。
    """
    # 公共基础配置
    base_config = {
        "conn_id": "spark3_on_yarn",
        "config_kwargs": {
            "spark.kerberos.keytab": "/appl/hadoop/cdp/keytabs/etl_user.keytab",
            "spark.kerberos.principal": "etl_user@AIDA.COM",
            "spark.executorEnv.PYTHONPATH": "/opt/cloudera/parcels/SPARK3/lib/spark3/python:/opt/cloudera/parcels/SPARK3/lib/spark3/python/lib/py4j-0.10.9.7-src.zip"
        }
    }

    # 单独处理 config_kwargs 的合并
    if "config_kwargs" in override_kwargs:
        base_config["config_kwargs"].update(override_kwargs.pop("config_kwargs"))

    # 其余参数（task_id, conn_id 等）直接覆盖
    base_config.update(override_kwargs)

    # 返回应用了配置的装饰器
    return task.pyspark(**base_config)



@task(task_id="DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_ROOT")
def DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_ROOT(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys() if kw_args else []
    return "({})".format(",".join(keys))


@task(task_id="Job_VIEW")
def DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys() if kw_args else []
    return "({})".format(",".join(keys))


@common_pyspark_task(task_id="ORA_STAGE")
def DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_ORA_STAGE(**kw_args):
    # stage_type_id: OracleConnectorPX
    spark = kw_args.get('spark')
    dag = kw_args.get('dag').dag_id
    run_id = kw_args.get('dag_run').run_id.replace(':', '-').replace('+', '_')
    config = DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_Parameters(**kw_args)


    oracle_conn = BaseHook.get_connection("oracle_107")
    jdbc_schema = oracle_conn.extra_dejson.get("service_name") or oracle_conn.schema
    jdbc_url = f'jdbc:oracle:thin:@{oracle_conn.host}:{oracle_conn.port or 1521}/{jdbc_schema}'
    jdbc_user = oracle_conn.login
    jdbc_password = oracle_conn.password
    driver = 'oracle.jdbc.driver.OracleDriver'

    TRANSFORMED = spark.read.format('jdbc').option('url', jdbc_url).option('query', f""" select a.*, current_date as CDC_TMSP from {config["STCDB_Schema_IBROKER"]}.{config["SrcTB"]} a """).option('user', jdbc_user).option('password', jdbc_password).option('driver', driver).load()

    ORA_STAGE_L_ORA_STAGETB = TRANSFORMED

    print('ORA_STAGE_L_ORA_STAGETB')
    print(ORA_STAGE_L_ORA_STAGETB.schema.json())
    print('count:{}'.format(ORA_STAGE_L_ORA_STAGETB.count()))
    ORA_STAGE_L_ORA_STAGETB.show(1000, False)
    ORA_STAGE_L_ORA_STAGETB.write.mode('overwrite').parquet(f'/tmp/airflow_output/{dag}/{run_id}/ORA_STAGE_L_ORA_STAGETB')


@common_pyspark_task(task_id="CP_STAGE_TGTTB")
def DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_CP_STAGE_TGTTB(**kw_args):
    # stage_type_id: PxCopy
    spark = kw_args.get('spark')
    dag = kw_args.get('dag').dag_id
    run_id = kw_args.get('dag_run').run_id.replace(':', '-').replace('+', '_')
    config = DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_Parameters(**kw_args)
    L_ORA_STAGETB = spark.read.parquet(f'/tmp/airflow_output/{dag}/{run_id}/ORA_STAGE_L_ORA_STAGETB')

    TRANSFORMED = L_ORA_STAGETB

    CP_STAGE_TGTTB_DSLink29 = TRANSFORMED

    print('CP_STAGE_TGTTB_DSLink29')
    print(CP_STAGE_TGTTB_DSLink29.schema.json())
    print('count:{}'.format(CP_STAGE_TGTTB_DSLink29.count()))
    CP_STAGE_TGTTB_DSLink29.show(1000, False)
    CP_STAGE_TGTTB_DSLink29.write.mode('overwrite').parquet(f'/tmp/airflow_output/{dag}/{run_id}/CP_STAGE_TGTTB_DSLink29')


@common_pyspark_task(task_id="ORA_TGT")
def DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_ORA_TGT(**kw_args):
    # stage_type_id: OracleConnectorPX
    spark = kw_args.get('spark')
    dag = kw_args.get('dag').dag_id
    run_id = kw_args.get('dag_run').run_id.replace(':', '-').replace('+', '_')
    config = DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_Parameters(**kw_args)
    DSLink29 = spark.read.parquet(f'/tmp/airflow_output/{dag}/{run_id}/CP_STAGE_TGTTB_DSLink29')

    oracle_conn = BaseHook.get_connection("oracle_107")
    jdbc_schema = oracle_conn.extra_dejson.get("service_name") or oracle_conn.schema
    jdbc_url = f'jdbc:oracle:thin:@{oracle_conn.host}:{oracle_conn.port or 1521}/{jdbc_schema}'
    jdbc_user = oracle_conn.login
    jdbc_password = oracle_conn.password
    driver = 'oracle.jdbc.driver.OracleDriver'

    dbtable = f'{config["ODSDB_Schema"]}.{config["TgtTB"]}'
    oracle_hook = OracleHook(oracle_conn_id="oracle_107")
    DSLink29.write.format('jdbc') \
        .option('url', jdbc_url) \
        .option('dbtable', dbtable) \
        .option('user', jdbc_user) \
        .option('password', jdbc_password) \
        .option('driver', driver) \
        .mode('append').save()



def DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_Parameters(**kwargs):
    """统一的配置获取函数"""
    params = kwargs.get('params', {})
    dag_run = kwargs.get('dag_run')

    # 统一的获取逻辑：优先dag_run.conf，回退到params
    if dag_run and dag_run.conf and 'PROJDEF' in dag_run.conf:
        projdef = dag_run.conf.get('PROJDEF', {})
    else:
        projdef = params.get('PROJDEF', {})

    config = {
        'ODSDB_Server': projdef.get('ODSDB_Server'),
        'ODSDB_User': projdef.get('ODSDB_User'),
        'ODSDB_Schema': projdef.get('ODSDB_Schema'),
        'ODS_DateSchema': projdef.get('ODS_DateSchema'),
        'SrcTB': projdef.get('SrcTB'),
        'TgtTB': projdef.get('TgtTB'),
        'APT_STARTUP_STATUS': projdef.get('APT_STARTUP_STATUS'),
        'STCDB_User_IBROKER': projdef.get('STCDB_User_IBROKER'),
        'STCDB_Server_IBROKER': projdef.get('STCDB_Server_IBROKER'),
        'STCDB_Schema_IBROKER': projdef.get('STCDB_Schema_IBROKER'),
        'ODSIFF_Outbox': projdef.get('ODSIFF_Outbox'),
    }
    return config


####################################[Main: DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND]###################################
with DAG(
    dag_id="adam_DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND",
    start_date=pendulum.datetime(2001, 1, 1, 9, 0, 0, tz='Asia/Hong_Kong'),
    schedule=None,
    catchup=False,
    tags=['adam'],
    params={
        "PROJDEF": Param(
            default={
                    "ODSDB_Server": "",
                    "ODSDB_User": "",
                    "ODSDB_Schema": "airflow",
                    "ODS_DateSchema": "",
                    "SrcTB": "BO_CHANNEL",
                    "TgtTB": "LV1_BO_CHANNEL",
                    "APT_STARTUP_STATUS": "True",
                    "STCDB_User_IBROKER": "",
                    "STCDB_Server_IBROKER": "",
                    "STCDB_Schema_IBROKER": "airflow",
                    "ODSIFF_Outbox": "",
                },
            type="object",
            description="Project configuration (can be overridden via dag_run.conf)"
        )
    }
) as dag_DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND:
    
    DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_ROOT_inst = DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_ROOT()
    Job_VIEW_inst = DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_Job_VIEW()
    ORA_STAGE_inst = DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_ORA_STAGE()
    CP_STAGE_TGTTB_inst = DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_CP_STAGE_TGTTB()
    ORA_TGT_inst = DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_ORA_TGT()

    CP_STAGE_TGTTB_inst >> ORA_TGT_inst
    DATA_REP_SINGLE_TB_IBROKER_CLNDAYEND_ROOT_inst >> Job_VIEW_inst
    Job_VIEW_inst >> ORA_STAGE_inst
    ORA_STAGE_inst >> CP_STAGE_TGTTB_inst

