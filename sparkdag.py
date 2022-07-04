import airflow 
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
dagconf = DAG (
    dag_id="sparksubmit", schedule_interval='****',
    start_date = airflow.utils.dates.days_ago(2)

)
spark_submit_local = SparkSubmitOperator(
    task_id="sparksubmitjob",
    application = '/home/sam/trg_project/sparktransfer.py',
    conn_id = 'spark_default', dag=dagconf)

spark_submit_local
if __name__=='__main__':
    dagconf.cli()
