# NYC Taxi Big Data Pipeline

Projet académique Big Data : déploiement d’une **infrastructure Big Data distribuée** (Spark + MinIO + PostgreSQL) via **Docker Compose**, puis implémentation progressive des exercices **ex01 → ex06**.

L’objectif est de construire un **pipeline Big Data complet**, de la collecte des données jusqu’à l’orchestration automatisée avec Airflow.

---

## Architecture globale (Partie 1 – Infrastructure)

### Spark Cluster (Docker Linux)
- Cluster Spark **distribué**
  - 1 Spark Master
  - 2 Spark Workers
- Exécution **exclusivement via `spark-submit`**
- Aucune exécution Spark locale sur Windows
- Interfaces :
  - Spark Master UI : http://localhost:8081
  - Worker 1 UI : http://localhost:8082
  - Worker 2 UI : http://localhost:8083

### MinIO – Data Lake (S3 compatible)
- Console MinIO : http://localhost:9001
- API S3 : http://localhost:9000
- Buckets utilisés :
  - `nyc-raw` → Raw Zone (données brutes)
  - `nyc-interim` → Processed Zone (données nettoyées/intermédiaires)
  - `nyc-processed` → Curated Zone (données finales)

### PostgreSQL – Data Warehouse
- SGBD : PostgreSQL
- Host : `localhost`
- Port : `5432`
- Base par défaut : `nyc_dw`

---

## Structure du projet

```
.
├── Docker/
├── data/
│   ├── raw/
│   ├── interim/
│   ├── processed/
│   └── external/
├── ex01_data_retrieval/
├── ex02_data_ingestion/
├── ex03_sql_table_creation/
├── ex04_dashboard/
├── ex05_ml_prediction_service/
├── ex06_airflow/
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## Contraintes techniques respectées

- OS hôte : **Windows**
- Spark exécuté **uniquement dans des conteneurs Linux**
- Jobs Spark lancés **exclusivement via `spark-submit`**
- Scala **2.13.x**, Spark **3.5.x**, Java **8**
- Données et volumes Docker stockés sur **SSD externe (S:)**

---

## Pré-requis

- Docker Desktop (WSL2)
- Git
- PowerShell

---

## Configuration des chemins (SSD externe)

Créer le fichier `.env` à partir du template :

```powershell
Copy-Item .env.example .env
```

Exemple de configuration :

```env
PROJECT_ROOT=S:/PROJECTS/projects/nyc-taxi-bigdata-pipeline
DOCKER_VOLUMES_ROOT=S:/PROJECTS/docker/volumes

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123

POSTGRES_DB=nyc_dw
POSTGRES_USER=nyc_user
POSTGRES_PASSWORD=nyc_password
```

---

## Lancer l’infrastructure

```powershell
docker compose up -d
```

---

## Exercice 01 – Data Retrieval (VALIDÉ)

- Dataset : NYC Yellow Taxi – Janvier 2023
- Source officielle NYC TLC
- Stockage local :
  `data/raw/yellow/2023/01/`
- Stockage MinIO :
  `s3a://nyc-raw/yellow/2023/01/`

---

## État du projet

| Partie | Statut |
|------|--------|
| Infrastructure | ✅ |
| Ex01 – Data Retrieval | ✅ |
| Ex02 – Ingestion | ⏳ |
| Ex03 – Data Warehouse | ⏳ |
| Ex04 – Dashboard | ⏳ |
| Ex05 – Machine Learning | ⏳ |
| Ex06 – Airflow | ⏳ |

---

## Auteur
Projet réalisé dans le cadre du cours **Big Data – CY Tech**.
