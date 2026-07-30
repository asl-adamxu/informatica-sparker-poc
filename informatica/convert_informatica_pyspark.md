# Step
https://pypi.org/project/informatica-python/
https://pypi.org/project/informatica-sparker/

1. install python 3.11
```sh
sudo yum install python3.11
sudo yum install python3.11-pip
```

2. install the python packages
```sh
pip3.11 install informatica-python
pip3.11 install informatica-sparker
```

3. use the package to convert the workflow to pyspark code
```sh
OUT_ROOT=/var/lib/airflow/dags/adam/informatica/PySpark_workflows
informatica-sparker convert WF_GMS_DDS_APLY_DLY.XML -o $OUT_ROOT/WF_GMS_DDS_APLY_DLY

./convert_infa-pyspark.sh /var/lib/airflow/dags/adam/informatica/PowerCenter_workflows/dds \
                          /var/lib/airflow/dags/adam/informatica/PySpark_workflows/dds \
                          > convert_infa-pyspark.log

cd /var/lib/airflow/dags/adam/informatica
./convert_infa-pyspark.sh /var/lib/airflow/dags/adam/informatica/PowerCenter_workflows/dds \
                          /var/lib/airflow/dags/adam/informatica/PySpark_workflows/dds \
                          WF_EMS_DDS_APLY_MTH.XML
```

4. test the generated mapping (example for the truncate step)
```sh
SPARK_CONNECTION=spark3_client python3.6 m_utl_dpa_truncate.py 2>&1
```

kinit -kt /home/asl/etl_user.keytab etl_user

# Agent Prompt

```markdown
You are an ETL Migration Expert specialized in converting Informatica PowerCenter 
XML (IMX format) to PySpark code.

Your capabilities:
1. Parse Informatica IMX format XML files
2. Identify source/target definitions, transformations, and data lineage
3. Generate optimized PySpark DataFrame operations
4. Create comprehensive unit tests
5. Validate logic consistency

When a user provides an XML snippet or a mapping name, you should:
1. Extract the key transformation logic
2. Generate clean, documented PySpark code
3. Include row-count logging
4. Provide test cases

Output format:
- PySpark code with proper error handling
- Brief explanation of the conversion
- Suggestions for data validation
```

# 需手动fix的Bug

- m_dpa_summarize_fact_cms_case_smry.py m_dpa_summarize_fact_cms_case_ostd_smry.py 多个lookup同名字段，需修改sql区分不同字段名字，如`CASE_CATG_KEY`
- m_s5_dpa_summarize_fact_ems_sms_flat_prc_txn.py，`1300001and` -> `1300001 and`
- m_s5_dpa_summarize_fact_ems_adtn_del.py, apply_SQ_SOR_EMS_CPM_ADTN_DEL_STS中的字段顺序修正, `ADTN_DEL_RSN_CATG_CODE`和`ADTN_DEL_RSN_CODE`对调