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

./convert_infa-pyspark.sh /var/lib/airflow/dags/adam/informatica/PowerCenter_workflows/extract \
                          /var/lib/airflow/dags/adam/informatica/PySpark_workflows/extract \
                          > convert_infa-pyspark.log

cd /var/lib/airflow/dags/adam/informatica
./convert_infa-pyspark.sh /var/lib/airflow/dags/adam/informatica/PowerCenter_workflows/dds \
                          /var/lib/airflow/dags/adam/informatica/PySpark_workflows/dds \
                          WF_CMS_DDS_APLY_MTH.XML
```

4. test the generated mapping (example for the truncate step)
```sh
python m_utl_dpa_truncate.py 2>&1
```

```sh
python3.11  m_s5_dpa_summarize_fact_ems_prh_abu.py
yarn application -list -appStates ALL -appTypes SPARK | grep -i "M_S5_DPA_SUMMARIZE_FACT_EMS_PRH_ABU"
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
  - m_s5_dds_aply_fact_ems_sms_aply_type_txn.py, `RLS_CNTL_DMNS_TYPE_CODE` -> `DDS_RLS_CNTL_DMNS_TYPE_CODE`
- 数字转字符可能出现科学计数法问题，如rec_rls_ind，要显式定义decimal类型
- 
