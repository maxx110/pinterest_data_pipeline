import airflow
from airflow.operators.python_operator import PythonOperator
#from airflow import utils
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
sys.path.append('/home/callum/pinterest_data_pipeline/transformation')
import spark_transformation

# Define default_args dictionary to specify default parameters of the DAG, such as the start date, frequency, etc.
default_args = {
    'owner': 'me',
    'start_date': days_ago(2),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create a DAG object and pass it the default_args dictionary
dag = airflow.DAG(
    'my_dag_id',
    default_args=default_args,
    schedule_interval='* * * * *'
)

# Define a function to run script A
def transform():
    spark_transformation().run_prog()
    pass

# # Define a function to run script B
# def run_script_b():
#     # insert code to run script B here
#     pass

# # Define a function to run script C
# def run_script_c():
#     # insert code to run script C here
#     pass

# Create PythonOperator objects to run the functions defined above
run_script_a_task = PythonOperator(
    task_id='run_script_a',
    python_callable=transform,
    dag=dag
)

# run_script_b_task = PythonOperator(
#     task_id='run_script_b',
#     python_callable=run_script_b,
#     dag=dag
# )

# run_script_c_task = PythonOperator(
#     task_id='run_script_c',
#     python_callable=run_script_c,
#     dag=dag
# )

# Set the dependencies between the tasks: script A should run first, followed by script B and then script C
run_script_a_task #>> run_script_b_task >> run_script_c_task
