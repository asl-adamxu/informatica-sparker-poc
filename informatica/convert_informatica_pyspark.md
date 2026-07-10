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

./convert_infa-pyspark.sh /var/lib/airflow/dags/adam/informatica/PowerCenter_workflows \
                          /var/lib/airflow/dags/adam/informatica/PySpark_workflows \
                          > convert_infa-pyspark.log

cd /var/lib/airflow/dags/adam/informatica
./convert_infa-pyspark.sh /var/lib/airflow/dags/adam/informatica/PowerCenter_workflows \
                          /var/lib/airflow/dags/adam/informatica/PySpark_workflows \
                          WF_EMS_PRHE_DDS_APLY_RVN_MTH.XML
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
