import os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.exceptions import AirflowFailException
from datetime import date, datetime, timedelta

#Initializations
CONDA_PATH = "/home/hadoop/miniconda3/envs/sparkenv/bin/python"
SPARK_PATH = "/home/hadoop/spark/bin/spark-submit"
yesterday_date = date.today() - timedelta(days=1)

home_dir = "/home/hadoop/ecommerce-data-pipeline/"
hdfs_dir = "/user/hive/warehouse/ecommerce"

hdfs_ec_path = os.path.join(hdfs_dir, f"event_count/dt={yesterday_date}/")
hdfs_pc_path = os.path.join(hdfs_dir, f"product_count/dt={yesterday_date}/")
hdfs_te_path = os.path.join(hdfs_dir, f"time_events/dt={yesterday_date}/")

# Default Arguments for DAG Process
default_args = {
    'owner':'Sammy',
    'depends_on_past':False,
    'start_date': datetime(2025,1,1),
    'trigger_rule' : 'one_failed',
    'retries':0
}

#Function to check if Hadoop and Hive are running
def check_running():
    from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
    try:
        hive_hook = HiveServer2Hook(hiveserver2_conn_id='hive_cli_default')
        hive_hook.get_records("SHOW DATABASES")
        hdfs_hook = WebHDFSHook(webhdfs_conn_id='webhdfs_default')
        hdfs_hook.check_for_path("/")
        print("Services active via Hooks")
    except Exception as e:
        raise AirflowFailException (str(e))

#Function to check if Hive tables are present
def check_hive():
    server_hook = HiveServer2Hook(HiveServer2Hook_conn_id='hive_cli_default')
    tables = ['event_count', 'product_count', 'time_events']
    missing = []
    for i in tables:
        sql_query = f"SHOW TABLES IN ecommerce LIKE '{i}'"
        if server_hook.get_first(sql_query):
            print(f"Found {i}")
        else:
            missing.append(i)
    
    if missing:
        raise AirflowFailException(f"Tables missing:{missing}. Create these tables in Beeline.")


#DAG Function
with DAG (
    dag_id="ecommerce_data_flow",
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:   
    
    #Task 0: Check if Hadoop and Hive are both running
    check_systems = PythonOperator(
        task_id = "task_0_check_hadoop_hive",
        python_callable = check_running
    )

    # Task 1: Generating Logs
    task1_path = os.path.join(home_dir, "data-generator/log-generator.py")
    etl_task_one = BashOperator(
        task_id = "task_1_run_log_generator",
        bash_command = f"{CONDA_PATH} {task1_path}"
    )

    # Task 2: Processing Logs made in Task 1
    task2_path = os.path.join(home_dir, "pyspark-jobs/process_logs.py")
    etl_task_two = BashOperator(
        task_id = "task_2_run_log_processing",
        bash_command = f"{SPARK_PATH} {task2_path}"
    )
    #Task 3: HDFS Operations (Creating Folders and Adding Files)
    ec_op_path = os.path.join(home_dir, "output/event_count/*")
    pc_op_path = os.path.join(home_dir, "output/product_count/*")
    te_op_path = os.path.join(home_dir, "output/time_events/*")
    etl_task_three = BashOperator(
        task_id = "task_3_hdfs_ops",
        bash_command = f"""
        hdfs dfs -mkdir -p {hdfs_ec_path}
        hdfs dfs -mkdir -p {hdfs_pc_path}
        hdfs dfs -mkdir -p {hdfs_te_path}
        hdfs dfs -rm {hdfs_ec_path}*
        hdfs dfs -rm {hdfs_pc_path}*
        hdfs dfs -rm {hdfs_te_path}*
        hdfs dfs -put {ec_op_path} {hdfs_ec_path}
        hdfs dfs -put {pc_op_path} {hdfs_pc_path}
        hdfs dfs -put {te_op_path} {hdfs_te_path}
        """
    )

    #Task 4: Check if Hive has tables
    check_tables = PythonOperator(
        task_id = "task_4_check_tables_in_hive",
        python_callable = check_hive
    )

    #Task 5: Updating Hive Tables
    etl_task_four = HiveOperator(
        task_id = "task_5_hive_update_tables",
        hive_cli_conn_id = 'hive_cli_default',
        hql = """
        MSCK REPAIR TABLE ecommerce.event_count SYNC PARTITIONS;
        MSCK REPAIR TABLE ecommerce.product_count SYNC PARTITIONS;
        MSCK REPAIR TABLE ecommerce.time_events SYNC PARTITIONS;
        """
    )

#Airflow DAG Order
check_systems >> etl_task_one >> etl_task_two  >> etl_task_three >> check_tables >> etl_task_four