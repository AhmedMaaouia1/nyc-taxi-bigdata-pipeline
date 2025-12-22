# NYC Taxi Big Data Pipeline

Projet académique personnel : déploiement d'une infra Big Data (Spark + MinIO + PostgreSQL) via Docker Compose, puis exercices ex01 → ex06.

## Architecture (Partie 1)
- Spark Cluster distribué (Docker Linux)
  - 1 master + 2 workers
  - Spark UI master: http://localhost:8080
  - Worker UIs: http://localhost:8081 / http://localhost:8082
- MinIO (S3 Data Lake)
  - Console: http://localhost:9001
  - API S3: http://localhost:9000
  - Buckets: `nyc-raw`, `nyc-interim`, `nyc-processed`
- PostgreSQL (Data Warehouse)
  - Host: localhost
  - Port: 5432
  - DB: `nyc_dw` (par défaut)

## Contraintes respectées
- OS hôte: Windows
- Spark tourne uniquement dans Docker Linux
- Lancement de jobs uniquement via `spark-submit`
- Volumes Docker et données lourdes sur SSD externe (S:)

## Pré-requis
- Docker Desktop (WSL2)
- Git
- PowerShell

## Configuration des chemins (SSD)
Créer `.env` depuis le template :

```powershell
Copy-Item .env.example .env
