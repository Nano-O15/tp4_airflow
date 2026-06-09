import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    "owner": "oukhemanou",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def get_villes() -> dict:
    default = json.dumps({
        "Paris": [48.8566, 2.3522],
        "Lyon": [45.7640, 4.8357],
        "Marseille": [43.2965, 5.3698],
    })
    raw = Variable.get("meteo_villes", default_var=default)
    return json.loads(raw)


def get_table_cible() -> str:
    return Variable.get("meteo_table", default_var="observations_meteo")


def fetch_meteo(ville: str, latitude: float, longitude: float, **context):
    hook = HttpHook(method="GET", http_conn_id="open_meteo_api")

    params = {
        "latitude":  latitude,
        "longitude": longitude,
        "current":   "temperature_2m,wind_speed_10m,precipitation",
        "timezone":  "Europe/Paris",
    }

    logging.info(f"[fetch] Appel API pour {ville}")
    response = hook.run(endpoint="/v1/forecast", data=params)
    raw_data = response.json()
    logging.info(f"[fetch] Réponse brute {ville} : {json.dumps(raw_data, indent=2)}")

    context["ti"].xcom_push(key=f"raw_{ville}", value=raw_data)


def transform_data(ville: str, **context):
    ti = context["ti"]
    raw_data = ti.xcom_pull(key=f"raw_{ville}", task_ids=f"fetch_meteo_{ville}")

    if not raw_data:
        raise ValueError(f"[transform] Aucune donnée brute pour {ville}.")

    current = raw_data.get("current")
    if not current:
        raise ValueError(f"[transform] Clé 'current' absente pour {ville}.")

    champs_requis = ["temperature_2m", "wind_speed_10m", "precipitation", "time"]
    manquants = [c for c in champs_requis if c not in current]
    if manquants:
        raise ValueError(f"[transform] Champs manquants pour {ville} : {manquants}")

    transformed = {
        "ville": ville,
        "heure": current["time"],
        "temperature_c": current["temperature_2m"],
        "vent_kmh": current["wind_speed_10m"],
        "precipitation_mm": current["precipitation"],
        "date_execution": context["ds"],
    }

    logging.info(f"[transform] {ville} → {transformed}")
    ti.xcom_push(key=f"transformed_{ville}", value=transformed)


def load_data(ville: str, **context):
    ti = context["ti"]
    transformed = ti.xcom_pull(
        key=f"transformed_{ville}", task_ids=f"transform_data_{ville}"
    )

    if not transformed:
        raise ValueError(f"[load] Aucune donnée transformée pour {ville}.")

    table = get_table_cible()
    hook  = PostgresHook(postgres_conn_id="postgres_meteo")

    sql = f"""
        INSERT INTO {table}
            (ville, heure, temperature_c, vent_kmh, precipitation_mm, date_execution)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (ville, heure) DO NOTHING;
    """

    hook.run(sql, parameters=(
        transformed["ville"],
        transformed["heure"],
        transformed["temperature_c"],
        transformed["vent_kmh"],
        transformed["precipitation_mm"],
        transformed["date_execution"],
    ))

    logging.info(f"[load] {ville} inséré dans {table}.")


def log_ingestion(ville: str, **context):
    ti   = context["ti"]
    hook = PostgresHook(postgres_conn_id="postgres_meteo")

    transformed = ti.xcom_pull(
        key=f"transformed_{ville}", task_ids=f"transform_data_{ville}"
    )
    statut  = "success" if transformed else "failure"
    message = None if statut == "success" else f"Aucune donnée transformée pour {ville}."

    sql = """
        INSERT INTO log_ingestion
            (dag_id, task_id, ville, statut, message, date_execution)
        VALUES
            (%s, %s, %s, %s, %s, %s);
    """

    hook.run(sql, parameters=(
        context["dag"].dag_id,
        f"log_ingestion_{ville}",
        ville,
        statut,
        message,
        context["ds"],
    ))

    logging.info(f"[log_ingestion] {ville} → statut={statut}")


def alert_on_failure(**context):
    logging.error("[alert] ÉCHEC détecté dans le pipeline météo PostgreSQL.")
    logging.error(f"[alert] Date : {context['ds']}")


def log_execution(**context):
    villes = list(get_villes().keys())
    logging.info("=" * 60)
    logging.info(f"[log] openmeteo_ingestion_postgres — bilan d'exécution")
    logging.info(f"[log] Date : {context['ds']} | Villes : {villes}")
    logging.info("=" * 60)


with DAG(
    dag_id="openmeteo_ingestion_postgres",
    description="Ingestion Open-Meteo PostgreSQL — fetch / transform / load par ville",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["open-meteo", "tp4"],
) as dag:

    toutes_taches = []
    VILLES = get_villes()

    for ville, coords in VILLES.items():
        latitude, longitude = coords[0], coords[1]

        fetch = PythonOperator(
            task_id=f"fetch_meteo_{ville}",
            python_callable=fetch_meteo,
            op_kwargs={"ville": ville, "latitude": latitude, "longitude": longitude},
        )

        transform = PythonOperator(
            task_id=f"transform_data_{ville}",
            python_callable=transform_data,
            op_kwargs={"ville": ville},
        )

        load = PythonOperator(
            task_id=f"load_data_{ville}",
            python_callable=load_data,
            op_kwargs={"ville": ville},
        )

        log_ing = PythonOperator(
            task_id=f"log_ingestion_{ville}",
            python_callable=log_ingestion,
            op_kwargs={"ville": ville},
            trigger_rule="all_done",
        )

        fetch >> transform >> load >> log_ing

        toutes_taches += [fetch, transform, load, log_ing]

    alerte = PythonOperator(
        task_id="alert_on_failure",
        python_callable=alert_on_failure,
        trigger_rule="one_failed",
    )

    log_fin = PythonOperator(
        task_id="log_execution",
        python_callable=log_execution,
        trigger_rule="all_done",
    )

    toutes_taches >> alerte
    toutes_taches >> log_fin