from datetime import datetime, timedelta
import imp
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

local_tz = pendulum.timezone("Asia/Tehran")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10, tzinfo=local_tz),
    'email': ['murtazanaiyar@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='Data-Pipeline',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 * * * *")
# pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
# x=get_avg_salary_agency()

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

def process_source_file(path):
  df=pd.read_csv(path,header=True)
  df.columns= df.columns.str.lower()
  df.columns=df.columns.str.replace(' ','_')
  df.rename(columns={'#_of_positions':'no_of_positions'}, inplace=True)
  df.to_csv('/dataset/output/processed/cleanedfile.csv',header=True)


process_source_file = PythonOperator(
    task_id='process_source_file',
    python_callable= process_source_file,
    op_kwargs = {"path" : "/dataset/nyc-jobs.csv"},
    dag=dag,
)

top_n_job_posting = SparkSubmitOperator(task_id='top_n_job_posting',
                                              conn_id='spark_local',
                                              application=f'dag_get_top_n_job_posting.py',
                                              total_executor_cores=4,
                                              executor_cores=2,
                                              executor_memory='5g',
                                              driver_memory='5g',
                                              conf={'master':'spark://master:7077'},
                                              execution_timeout=timedelta(minutes=10),
                                              dag=dag
                                              )

avg_salary_agency = SparkSubmitOperator(task_id='avg_salary_agency',
                                              conn_id='spark_local',
                                              application=f'dag_get_avg_salary_agency.py',
                                              total_executor_cores=4,
                                              executor_cores=2,
                                              executor_memory='5g',
                                              driver_memory='5g',
                                              conf={'master':'spark://master:7077'},
                                              execution_timeout=timedelta(minutes=10),
                                              dag=dag
                                              )

avg_salary_per_degree = SparkSubmitOperator(task_id='avg_salary_per_degree',
                                              conn_id='spark_local',
                                              application=f'dag_get_avg_salary_per_degree.py',
                                              total_executor_cores=4,
                                              executor_cores=2,
                                              executor_memory='5g',
                                              driver_memory='5g',
                                              conf={'master':'spark://master:7077'},
                                              execution_timeout=timedelta(minutes=10),
                                              dag=dag
                                              )

salary_per_category_per_frequency = SparkSubmitOperator(task_id='salary_per_category_per_frequency',
                                              conn_id='spark_local',
                                              application=f' dag_get_avg_salary_per_category_per_frequency.py',
                                              total_executor_cores=4,
                                              executor_cores=2,
                                              executor_memory='5g',
                                              driver_memory='5g',
                                              conf={'master':'spark://master:7077'},
                                              execution_timeout=timedelta(minutes=10),
                                              dag=dag
                                              )

top_job_posting_salary_agency = SparkSubmitOperator(task_id='top_job_posting_salary_agency',
                                              conn_id='spark_local',
                                              application=f'dag_top_job_posting_salary_agency.py',
                                              total_executor_cores=4,
                                              executor_cores=2,
                                              executor_memory='5g',
                                              driver_memory='5g',
                                              conf={'master':'spark://master:7077'},
                                              execution_timeout=timedelta(minutes=10),
                                              dag=dag
                                              )     

highest_paid_skill = SparkSubmitOperator(task_id='highest_paid_skill',
                                              conn_id='spark_local',
                                              application=f'dag_highest_paid_skill.py',
                                              total_executor_cores=4,
                                              executor_cores=2,
                                              executor_memory='5g',
                                              driver_memory='5g',
                                              conf={'master':'spark://master:7077'},
                                              execution_timeout=timedelta(minutes=10),
                                              dag=dag
                                              )                                                                                      

start >> process_source_file >> [salary_per_category_per_frequency,top_n_job_posting,avg_salary_agency,
avg_salary_per_degree,highest_paid_skill,top_job_posting_salary_agency]


[salary_per_category_per_frequency,top_n_job_posting,avg_salary_agency,
avg_salary_per_degree,highest_paid_skill,top_job_posting_salary_agency] >> end

