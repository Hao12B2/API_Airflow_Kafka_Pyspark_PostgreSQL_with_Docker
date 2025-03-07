import sys
sys.path.append('/opt/airflow')

from airflow import DAG
import airflow
from airflow.operators.python import PythonOperator
from jobs.python.extract_exchangeRate import finalExecutionExchangeRate

dag = DAG(
    dag_id= "spark_flow",
    default_args= {
        "owner" : "haonv",
        "start_date" : airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id = "start",
    python_callable = lambda: print("Jobs started"),
    dag = dag
)

extract_exchangeRate_python_job = PythonOperator(
    task_id = "extract_exchangeRate_python_job",
    python_callable = finalExecutionExchangeRate,
    dag = dag
)

end = PythonOperator(
    task_id = "end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag = dag
)

start >> extract_exchangeRate_python_job >> end