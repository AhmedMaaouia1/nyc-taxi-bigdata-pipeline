# NYC Taxi Big Data Pipeline

Projet Big Data visant à concevoir et implémenter un **pipeline Big Data complet**, depuis la collecte de données massives jusqu’à leur exploitation analytique et leur orchestration automatisée.

Le projet repose sur le déploiement d’une **infrastructure Big Data distribuée** basée sur **Spark, MinIO et PostgreSQL**, entièrement orchestrée via **Docker Compose**, puis sur la réalisation progressive des exercices **ex01 → ex06**.

---

## Architecture globale (Partie 1 – Infrastructure)

L’infrastructure est conçue pour respecter les contraintes suivantes :
- exécution exclusivement dans des **conteneurs Linux**
- aucune dépendance à une installation Spark locale
- compatibilité avec une orchestration future via **Airflow**
- stockage persistant sur **SSD externe**

### Spark Cluster (Docker Linux)

- Cluster Spark **distribué** déployé via Docker Compose
  - 1 Spark Master
  - 2 Spark Workers
- Versions :
  - Apache Spark **3.5.x**
  - Scala **2.13.x**
  - Java **8 (OpenJDK)**
- Exécution des traitements :
  - **uniquement via `spark-submit`**
  - aucun mode `local[*]`
- Les dossiers `data/` du projet sont montés dans tous les conteneurs Spark afin de garantir une vision cohérente du Data Lake.

Interfaces disponibles :
- Spark Master UI : http://localhost:8081
- Spark Worker 1 UI : http://localhost:8082
- Spark Worker 2 UI : http://localhost:8083

---

### MinIO – Data Lake (S3 compatible)

MinIO est utilisé comme **Data Lake S3-compatible**, permettant de simuler une architecture cloud tout en restant en local.

- Console MinIO : http://localhost:9001
- API S3 : http://localhost:9000
- Buckets utilisés :
  - `nyc-raw` → **Raw Zone** (données brutes telles que collectées)
  - `nyc-interim` → **Processed Zone** (données nettoyées/intermédiaires)
  - `nyc-processed` → **Curated Zone** (données finales prêtes à l’analyse)

Les buckets sont créés automatiquement au démarrage de l’infrastructure.

---

### PostgreSQL – Data Warehouse

PostgreSQL est utilisé comme **Data Warehouse relationnel**, destiné à accueillir les données structurées prêtes pour l’analyse SQL et la visualisation.

- SGBD : PostgreSQL
- Host : `localhost`
- Port : `5432`
- Base par défaut : `nyc_dw`
- Les données sont stockées sur volume persistant (SSD externe).

---

## Structure du projet

La structure du projet respecte strictement celle fournie dans l’énoncé et le dépôt de référence.

```text
.
├── Docker/                  # Dockerfile Spark + scripts de démarrage
├── data/                    # Data Lake local (zones Raw / Interim / Processed)
│   ├── raw/
│   ├── interim/
│   ├── processed/
│   └── external/
├── ex01_data_retrieval/     # Collecte des données NYC Taxi
├── ex02_data_ingestion/     # Ingestion et nettoyage (à venir)
├── ex03_sql_table_creation/ # Création du Data Warehouse (SQL)
├── ex04_dashboard/          # Visualisation / dashboard
├── ex05_ml_prediction_service/ # Machine Learning
├── ex06_airflow/            # Orchestration Airflow
├── docker-compose.yml
├── .env.example
└── README.md
```

⚠️ Les **données ne sont pas versionnées** dans Git (seule la structure des dossiers est conservée).

---

## Contraintes techniques respectées

- OS hôte : **Windows**
- Spark exécuté **uniquement dans des conteneurs Docker Linux**
- Lancement des jobs exclusivement via **`spark-submit`**
- Aucune exécution Spark locale
- Scala **2.13.x**, Spark **3.5.x**, Java **8**
- Volumes Docker et données stockés sur **SSD externe (S:)**
- Architecture modulaire et extensible (Airflow obligatoire en ex06)

---

## Pré-requis

- Docker Desktop (WSL2 activé)
- Git
- PowerShell
- Connexion Internet (téléchargement des datasets NYC Taxi)

---

## Configuration des chemins (SSD externe)

Les données et volumes Docker sont stockés sur un SSD externe afin d’éviter toute saturation du disque système.

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

Depuis la racine du projet :

```powershell
docker compose up -d
```

Vérifications :
- Spark Master et Workers visibles dans l’UI
- Accès à la console MinIO
- PostgreSQL opérationnel

---

## Exercice 01 – Data Retrieval (VALIDÉ)

### Objectif pédagogique
L’exercice EX01 a pour objectif de mettre en place la **collecte initiale des données** et leur intégration dans le Data Lake, sans transformation métier.

### Dataset utilisé
- NYC Yellow Taxi – **Janvier 2023**
- Source officielle : NYC Taxi & Limousine Commission (TLC)

### Étapes réalisées
1. Téléchargement du fichier Parquet depuis l’URL officielle
2. Stockage local dans la **Raw Zone**
3. Lecture du fichier via Spark
4. Écriture des données dans MinIO (bucket `nyc-raw`)

### Emplacements des données
- Stockage local :
  ```
  data/raw/yellow/2023/01/
  ```
- Stockage Data Lake (MinIO) :
  ```
  s3a://nyc-raw/yellow/2023/01/
  ```

Le job est **idempotent** : une relance n’écrase pas les données locales existantes et permet une exécution répétable (notamment via Airflow).

---

## État du projet

| Partie | Statut |
|------|--------|
| Infrastructure Docker | ✅ Terminée |
| Ex01 – Data Retrieval | ✅ Terminé |
| Ex02 – Ingestion multi-branche | ⏳ À venir |
| Ex03 – Data Warehouse | ⏳ À venir |
| Ex04 – Dashboard | ⏳ À venir |
| Ex05 – Machine Learning | ⏳ À venir |
| Ex06 – Airflow | ⏳ À venir |

---

## Auteur
Projet réalisé par **MAAOUIA Ahmed** dans le cadre du cours **Big Data – CY Tech**.
