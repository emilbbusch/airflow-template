from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

def get_starwars_api_data(endpoint: str):
    """Fetch data from the Star Wars API"""
    url = f"https://swapi.dev/api/{endpoint}/?format=json"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["results"]

def get_starwars_people(**kwargs):
    """Task to fetch people data"""
    ti = kwargs['ti']
    people = get_starwars_api_data("people")
    ti.xcom_push(key="people", value=people)

def get_starwars_planets(**kwargs):
    """Task to fetch planets data"""
    ti = kwargs['ti']
    planets = get_starwars_api_data("planets")
    ti.xcom_push(key="planets", value=planets)

def get_starwars_films(**kwargs):
    """Task to fetch films data"""
    ti = kwargs['ti']
    films = get_starwars_api_data("films")
    ti.xcom_push(key="films", value=films)

def starwars_statistics(**kwargs):
    """Task to compute statistics"""
    ti = kwargs['ti']
    people = ti.xcom_pull(task_ids='get_starwars_people', key="people")
    planets = ti.xcom_pull(task_ids='get_starwars_planets', key="planets")
    films = ti.xcom_pull(task_ids='get_starwars_films', key="films")
    
    stats = f"People: {len(people)}, Planets: {len(planets)}, Films: {len(films)}"
    print(stats)
    return stats

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='starwars_api_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    task_get_people = PythonOperator(
        task_id="get_starwars_people",
        python_callable=get_starwars_people,
        provide_context=True,
    )

    task_get_planets = PythonOperator(
        task_id="get_starwars_planets",
        python_callable=get_starwars_planets,
        provide_context=True,
    )

    task_get_films = PythonOperator(
        task_id="get_starwars_films",
        python_callable=get_starwars_films,
        provide_context=True,
    )

    task_statistics = PythonOperator(
        task_id="starwars_statistics",
        python_callable=starwars_statistics,
        provide_context=True,
    )

    [task_get_people, task_get_planets, task_get_films] >> task_statistics
