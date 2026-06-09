CREATE TABLE IF NOT EXISTS observations_meteo (
    id SERIAL PRIMARY KEY,
    ville VARCHAR(100) NOT NULL,
    heure TIMESTAMP NOT NULL,
    temperature_c NUMERIC(5, 2) NOT NULL,
    vent_kmh NUMERIC(6, 2) NOT NULL,
    precipitation_mm NUMERIC(6, 2) NOT NULL,
    date_execution DATE NOT NULL,
    UNIQUE (ville, heure)
);

CREATE TABLE IF NOT EXISTS log_ingestion (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(200) NOT NULL,
    task_id VARCHAR(200) NOT NULL,
    ville VARCHAR(100) NOT NULL,
    statut VARCHAR(20) NOT NULL,
    message TEXT,
    date_execution DATE NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);
