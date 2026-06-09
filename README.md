# TP4 — DAG Airflow : OpenMétéo - PostgreSQL

## Lancement de l'environnement

```bash
mkdir -p ./dags ./logs ./plugins
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up -d
```

> La commande `echo -e "AIRFLOW_UID=$(id -u)" > .env` concerne les OS Unix (Linux, macOS) pour éviter des problèmes de permissions sur les volumes partagés.

Accéder à l'interface web : [http://localhost:8080](http://localhost:8080) — login `airflow` / `airflow`

---

## Initialisation des tables PostgreSQL

```bash
docker exec -i airflow-tp4-postgres-1 psql -U airflow -d airflow < init_tables.sql
```

## Connexions à créer (`Admin → Connexions`)

| ID de Connexion | Type | Hôte | Identifiant | Mot de Passe | Port | Database |
|---|---|---|---|---|---|---|
| `open_meteo_api` | HTTP | `https://api.open-meteo.com` | — | — | — | — |
| `postgres_meteo` | Postgres | `postgres` | `airflow` | `airflow` | `5432` | `airflow` |

## Variables à créer (`Admin → Variables`)

| Clé | Valeur |
|---|---|
| `meteo_villes` | `{"Paris": [48.8566, 2.3522], "Lyon": [45.764, 4.8357], "Marseille": [43.2965, 5.3698]}` |
| `meteo_table` | `observations_meteo` |

## Déploiement du DAG

```bash
cp openmeteo_ingestion_postgres.py ./dags/
```

Le DAG `openmeteo_ingestion_postgres` apparaît dans l'interface web.  
Pour l'exécuter : activer le toggle puis cliquer sur **Déclancher**.

## Dépendances entre les tâches

Au sein de chaque ville, les tâches s'enchaînent séquentiellement :

```
fetch_meteo_[ville] >> transform_data_[ville] >> load_data_[ville] >> log_ingestion_[ville]
```

Les trois chaînes (une par ville) sont indépendantes et s'exécutent en parallèle. En fin de pipeline, toutes les tâches alimentent `alert_on_failure` et `log_execution` :

```
[fetch, transform, load, log_ingestion] x3 >> alert_on_failure
[fetch, transform, load, log_ingestion] x3 >> log_execution
```

## Description des tâches

Le pipeline récupère chaque jour les données météo de 3 villes (Paris, Lyon, Marseille) via l'API Open-Meteo (gratuite, sans clé). Les 3 chaînes s'exécutent en parallèle.

| Tâche | Rôle | Trigger rule |
|---|---|---|
| `fetch_meteo_[ville]` | Appel HTTP vers l'API Open-Meteo | défaut (`all_success`) |
| `transform_data_[ville]` | Extrait les champs métier et restructure pour la table cible | défaut (`all_success`) |
| `load_data_[ville]` | Insère les données dans PostgreSQL | défaut (`all_success`) |
| `log_ingestion_[ville]` | Écrit une ligne de suivi dans `log_ingestion` | `all_done` |
| `alert_on_failure` | Se déclenche si au moins une tâche échoue | `one_failed` |
| `log_execution` | Trace le bilan global d'exécution dans les logs Airflow | `all_done` |

Si `fetch_meteo_[ville]` échoue, les tâches `transform_data`, `load_data` et `log_ingestion` de cette ville passent en `UPSTREAM_FAILED`. `alert_on_failure` se déclenche immédiatement.

## Champs retenus pour la table cible

L'API Open-Meteo retourne de nombreux champs. On retient uniquement les 4 suivants, justifiés par le besoin métier :

| Champ API | Champ table cible | Unité | Justification |
|---|---|---|---|
| `temperature_2m` | `temperature_c` | °C | Indicateur météo principal |
| `wind_speed_10m` | `vent_kmh` | km/h | Donnée opérationnelle |
| `precipitation` | `precipitation_mm` | mm | Détection d'événements météo |
| `time` | `heure` | ISO 8601 | Clé temporelle de la table |

Les champs écartés (`interval`, `current_units`, `latitude`, `longitude`, etc.) sont soit des métadonnées API sans valeur métier, soit redondants avec les informations déjà présentes dans la table.

## Preuves d'exécution et de chargement

![Preuve d'exécution](./img/execution_success.png)

![Contenu de la table observations_meteo](./img/postgres_success_1.png)

![Contenu de la table log_ingestion](./img/postgres_success_2.png)
