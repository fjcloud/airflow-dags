from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import multiprocessing

def isprime(num):
    if num < 2:
        return None
    for i in range(2, num):
         if (num % i) == 0:
            return None
    else:
        return num

def calculate_primes(start, end):
    pool = multiprocessing.Pool(3)
    result = list(filter(lambda x: x is not None, pool.map(isprime, range(start, end)))
    return result

def run_prime_calculation(start, end):
    result = calculate_primes(start, end)
    return result

def create_dag(dag_id, start_range, end_range):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False)

    task_id = f'calculate_primes_{start_range}_{end_range}'
    task = PythonOperator(
        task_id=task_id,
        provide_context=True,
        python_callable=run_prime_calculation,
        op_args={'start': start_range, 'end': end_range},
        dag=dag,
    )

    return dag

default_args = {
    'owner': 'fjcloud',
    'start_date': datetime(2023, 10, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Nombre total de nombres premiers à calculer
total_primes = 1000

# Nombre maximal de nombres premiers à calculer par DAG
max_primes_per_dag = 1000

# Calcule le nombre de DAGs nécessaires
num_dags = (total_primes + max_primes_per_dag - 1) // max_primes_per_dag

# Crée les DAGs et les tâches
for dag_num in range(num_dags):
    start_range = dag_num * max_primes_per_dag
    end_range = min((dag_num + 1) * max_primes_per_dag, total_primes)
    dag_id = f'prime_calculator_{start_range}_{end_range}'
    dag = create_dag(dag_id, start_range, end_range)

if __name__ == "__main__":
    dag.cli()
