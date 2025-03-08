from airflow.decorators import dag, task
import requests
import pendulum


def get_starwars_api_data(endpoint: str):
    url = f"https://swapi.dev/api/{endpoint}/?format=json"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["results"]


@dag(
    dag_id="starwars_dag",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
)
def starwars_dag():
    @task()
    def get_starwars_people():
        return get_starwars_api_data("people")

    @task()
    def get_starwars_planets():
        return get_starwars_api_data("planets")

    @task()
    def get_starwars_films():
        return get_starwars_api_data("films")

    @task()
    def starwars_statistics(people, planets, films):
        return f"People: {len(people)}, Planets: {len(planets)}, Films: {len(films)}"

    people_data = get_starwars_people()
    planets_data = get_starwars_planets()
    films_data = get_starwars_films()

    starwars_statistics(people_data, planets_data, films_data)


starwars_dag()
