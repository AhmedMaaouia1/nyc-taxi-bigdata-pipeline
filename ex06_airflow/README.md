# Exercice 06 – Orchestration Airflow

## Objectif

L'exercice **EX06** a pour objectif de mettre en place l'**orchestration automatisée** de l'ensemble du pipeline Big Data à l'aide d'**Apache Airflow**.

## Architecture cible

```
┌─────────────────────────────────────────────────────────────────┐
│                        AIRFLOW                                  │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    DAG: nyc_taxi_pipeline                │  │
│  │                                                          │  │
│  │  ┌────────┐    ┌────────┐    ┌────────┐    ┌────────┐  │  │
│  │  │  EX01  │───▶│  EX02  │───▶│  EX03  │───▶│  EX04  │  │  │
│  │  │Retrieve│    │Ingest  │    │  DW    │    │  BI    │  │  │
│  │  └────────┘    └────────┘    └────────┘    └────────┘  │  │
│  │                     │                                    │  │
│  │                     ▼                                    │  │
│  │               ┌────────┐                                 │  │
│  │               │  EX05  │                                 │  │
│  │               │   ML   │                                 │  │
│  │               └────────┘                                 │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Composants Airflow

### Infrastructure

| Composant        | Description                              |
|------------------|------------------------------------------|
| Airflow Webserver| Interface web (port 8080)                |
| Airflow Scheduler| Planification des DAGs                   |
| Airflow Worker   | Exécution des tâches                     |
| PostgreSQL       | Backend metadata Airflow                 |
| Redis            | Message broker (CeleryExecutor)          |

### DAG principal

Le DAG `nyc_taxi_pipeline` orchestre l'ensemble du pipeline :

```
ex01_data_retrieval
        │
        ▼
ex02_data_ingestion
        │
   ┌────┴────┐
   ▼         ▼
ex03_dw   ex05_ml
   │
   ▼
ex04_dashboard_refresh
```

## Structure du projet (à implémenter)

```
ex06_airflow/
├── dags/
│   ├── nyc_taxi_pipeline.py      # DAG principal
│   ├── ex01_dag.py               # DAG EX01
│   ├── ex02_dag.py               # DAG EX02
│   └── common/
│       ├── spark_submit.py       # Helper spark-submit
│       └── config.py             # Configuration
├── plugins/
│   └── operators/
│       └── spark_submit_operator.py
├── docker-compose.airflow.yml    # Docker Compose Airflow
├── .env                          # Variables d'environnement
└── README.md
```

## Configuration

### Variables Airflow

| Variable              | Description                    |
|-----------------------|--------------------------------|
| `spark_master_url`    | URL Spark Master               |
| `minio_endpoint`      | URL MinIO                      |
| `postgres_conn_id`    | Connection ID PostgreSQL       |
| `default_year`        | Année par défaut               |
| `default_month`       | Mois par défaut                |

### Connections

| Connection ID   | Type       | Description           |
|-----------------|------------|-----------------------|
| `spark_default` | Spark      | Cluster Spark         |
| `postgres_dw`   | PostgreSQL | Data Warehouse        |
| `minio_s3`      | S3         | MinIO Data Lake       |

## Types de tâches

### SparkSubmitOperator

Pour les jobs Spark (EX01, EX02, EX05) :

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

ex01_task = SparkSubmitOperator(
    task_id='ex01_data_retrieval',
    application='/opt/workdir/ex01_data_retrieval/target/scala-2.12/ex01-data-retrieval_2.12-0.1.0.jar',
    conn_id='spark_default',
    java_class='Ex01DataRetrieval',
    application_args=['--year', '{{ ds[:4] }}', '--month', '{{ ds[5:7] }}'],
)
```

### PostgresOperator

Pour les scripts SQL (EX03) :

```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

ex03_load = PostgresOperator(
    task_id='ex03_dw_load',
    postgres_conn_id='postgres_dw',
    sql='dw_load_incremental.sql',
)
```

### BashOperator

Pour les scripts Python (EX05) :

```python
from airflow.operators.bash import BashOperator

ex05_train = BashOperator(
    task_id='ex05_ml_train',
    bash_command='docker exec spark-master spark-submit ... main.py --mode train',
)
```

## Scheduling

| DAG                  | Schedule       | Description              |
|----------------------|----------------|--------------------------|
| nyc_taxi_monthly     | `0 0 1 * *`    | 1er jour de chaque mois  |
| nyc_taxi_daily       | `0 6 * * *`    | Tous les jours à 6h      |
| nyc_taxi_manual      | `None`         | Déclenchement manuel     |

## Paramètres du DAG

```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'nyc_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi Big Data Pipeline',
    schedule_interval='@monthly',
    catchup=False,
    tags=['nyc-taxi', 'bigdata'],
)
```

## Docker Compose

```yaml
# docker-compose.airflow.yml
version: '3.8'

services:
  airflow-webserver:
    image: apache/airflow:2.7.0
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://...
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./plugins:/opt/airflow/plugins

  airflow-scheduler:
    image: apache/airflow:2.7.0
    # ...

  airflow-worker:
    image: apache/airflow:2.7.0
    # ...
```

## Monitoring

### Interface Web

- **URL** : http://localhost:8080
- **Credentials** : airflow / airflow (par défaut)

### Fonctionnalités

- 📊 Vue des DAGs et leur statut
- 📈 Graphiques d'exécution
- 📋 Logs des tâches
- 🔔 Alertes en cas d'échec
- 📅 Historique des runs

## Commandes utiles

```bash
# Démarrer Airflow
docker compose -f docker-compose.airflow.yml up -d

# Tester un DAG
airflow dags test nyc_taxi_pipeline 2023-01-01

# Lister les DAGs
airflow dags list

# Déclencher un DAG manuellement
airflow dags trigger nyc_taxi_pipeline

# Voir les logs
docker compose -f docker-compose.airflow.yml logs -f airflow-scheduler
```

## Dépendances

```
apache-airflow==2.7.0
apache-airflow-providers-apache-spark
apache-airflow-providers-postgres
apache-airflow-providers-amazon  # Pour S3/MinIO
```

## Prochaines étapes

1. [ ] Créer le fichier `docker-compose.airflow.yml`
2. [ ] Implémenter le DAG principal
3. [ ] Configurer les connections Airflow
4. [ ] Tester chaque tâche individuellement
5. [ ] Mettre en place les alertes
6. [ ] Documenter les procédures de recovery

## Statut

⏳ **À implémenter**

---

**Auteur :** MAAOUIA Ahmed – CY Tech Big Data
