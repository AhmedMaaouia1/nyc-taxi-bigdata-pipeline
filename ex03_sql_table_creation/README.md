# Exercice 03 – SQL Table Creation (Data Warehouse)

## Objectif

L'exercice **EX03** a pour objectif de concevoir et implémenter le **Data Warehouse** PostgreSQL selon un modèle en **étoile (Star Schema)**, adapté à l'analyse OLAP des trajets NYC Yellow Taxi.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    DATA WAREHOUSE (PostgreSQL)               │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│   ┌───────────┐  ┌───────────┐  ┌────────────┐              │
│   │ dim_date  │  │ dim_time  │  │dim_location│              │
│   └─────┬─────┘  └─────┬─────┘  └──────┬─────┘              │
│         │              │               │                     │
│         └──────────────┼───────────────┘                     │
│                        │                                     │
│                   ┌────┴────┐                                │
│                   │fact_trip│                                │
│                   └────┬────┘                                │
│                        │                                     │
│         ┌──────────────┼───────────────┐                     │
│         │              │               │                     │
│   ┌─────┴─────┐  ┌─────┴─────┐  ┌──────┴──────┐             │
│   │dim_vendor │  │dim_payment│  │ dim_ratecode│             │
│   └───────────┘  └───────────┘  └─────────────┘             │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## Modèle en étoile

### Table de faits : `fact_trip`

| Colonne               | Type             | Description                    |
|-----------------------|------------------|--------------------------------|
| trip_id               | BIGSERIAL (PK)   | Identifiant unique             |
| pickup_date           | DATE (FK)        | Date de prise en charge        |
| pickup_time           | TIME (FK)        | Heure de prise en charge       |
| pickup_location_id    | INTEGER (FK)     | Zone de départ                 |
| dropoff_location_id   | INTEGER (FK)     | Zone d'arrivée                 |
| vendor_id             | INTEGER (FK)     | Fournisseur                    |
| payment_type_id       | INTEGER (FK)     | Type de paiement               |
| ratecode_id           | INTEGER (FK)     | Code tarifaire                 |
| passenger_count       | INTEGER          | Nombre de passagers            |
| trip_distance         | DOUBLE PRECISION | Distance du trajet (miles)     |
| fare_amount           | DOUBLE PRECISION | Tarif de base                  |
| extra                 | DOUBLE PRECISION | Suppléments                    |
| mta_tax               | DOUBLE PRECISION | Taxe MTA                       |
| tip_amount            | DOUBLE PRECISION | Pourboire                      |
| tolls_amount          | DOUBLE PRECISION | Péages                         |
| improvement_surcharge | DOUBLE PRECISION | Surcharge d'amélioration       |
| congestion_surcharge  | DOUBLE PRECISION | Surcharge de congestion        |
| airport_fee           | DOUBLE PRECISION | Frais d'aéroport               |
| total_amount          | DOUBLE PRECISION | Montant total                  |

### Dimensions

#### `dim_date`
| Colonne     | Type    | Description              |
|-------------|---------|--------------------------|
| date_id     | DATE PK | Date                     |
| year        | INTEGER | Année                    |
| month       | INTEGER | Mois (1-12)              |
| day         | INTEGER | Jour du mois             |
| day_of_week | INTEGER | Jour de la semaine (0-6) |

#### `dim_time`
| Colonne | Type    | Description |
|---------|---------|-------------|
| time_id | TIME PK | Heure       |
| hour    | INTEGER | Heure (0-23)|
| minute  | INTEGER | Minute      |

#### `dim_location`
| Colonne      | Type       | Description        |
|--------------|------------|--------------------|
| location_id  | INTEGER PK | ID de zone TLC     |
| borough      | TEXT       | Arrondissement     |
| zone         | TEXT       | Nom de la zone     |
| service_zone | TEXT       | Zone de service    |

#### `dim_vendor`
| Colonne     | Type       | Description      |
|-------------|------------|------------------|
| vendor_id   | INTEGER PK | ID fournisseur   |
| vendor_name | TEXT       | Nom fournisseur  |

#### `dim_payment_type`
| Colonne             | Type       | Description         |
|---------------------|------------|---------------------|
| payment_type_id     | INTEGER PK | ID type paiement    |
| payment_description | TEXT       | Description         |

#### `dim_ratecode`
| Colonne             | Type       | Description         |
|---------------------|------------|---------------------|
| ratecode_id         | INTEGER PK | ID code tarifaire   |
| ratecode_description| TEXT       | Description         |

## Structure du projet

```
ex03_sql_table_creation/
├── staging_creation.sql      # Création table staging
├── dw_creation.sql           # Création dimensions + fact
├── dw_load_reference.sql     # Chargement données de référence
├── dw_load_incremental.sql   # Chargement depuis staging
├── indexes.sql               # Création des index
├── insertion.sql             # Scripts d'insertion
├── diagramme.mmd             # Diagramme Mermaid ER
├── diagram.png               # Diagramme exporté
└── data/
    └── taxi_zone_lookup.csv  # Données de référence zones
```

## Scripts SQL

### 1. Création du staging

```sql
-- staging_creation.sql
CREATE TABLE public.yellow_trips_staging (
  vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
  passenger_count, trip_distance, ratecodeid, ...
);
```

### 2. Création du Data Warehouse

```sql
-- dw_creation.sql
-- Création des dimensions et de la table de faits
```

### 3. Chargement des données de référence

```sql
-- dw_load_reference.sql
-- Insertion des valeurs connues (vendors, payment types, ratecodes)
```

### 4. Chargement incrémental

```sql
-- dw_load_incremental.sql
-- Alimentation depuis staging vers les dimensions et fact_trip
```

## Ordre d'exécution

```bash
# 1. Créer la table staging (une seule fois)
psql -f staging_creation.sql

# 2. Créer le schéma DW (une seule fois)
psql -f dw_creation.sql

# 3. Charger les données de référence (une seule fois)
psql -f dw_load_reference.sql

# 4. Après chaque EX02 avec --enableDw true :
psql -f dw_load_incremental.sql

# 5. Créer les index (après le premier chargement)
psql -f indexes.sql
```

## Données de référence

### Types de paiement
| ID | Description              |
|----|--------------------------|
| 0  | Flex Fare                |
| 1  | Credit card              |
| 2  | Cash                     |
| 3  | No charge                |
| 4  | Dispute                  |
| 5  | Unknown                  |
| 6  | Voided trip              |

### Codes tarifaires
| ID | Description              |
|----|--------------------------|
| 1  | Standard rate            |
| 2  | JFK                      |
| 3  | Newark                   |
| 4  | Nassau or Westchester    |
| 5  | Negotiated fare          |
| 6  | Group ride               |
| 99 | Unknown                  |

### Fournisseurs
| ID | Nom                         |
|----|-----------------------------|
| 1  | Creative Mobile Technologies|
| 2  | Curb Mobility               |
| 6  | Myle Technologies           |
| 7  | Helix                       |

## Technologies

| Composant       | Valeur          |
|-----------------|-----------------|
| SGBD            | PostgreSQL      |
| Modèle          | Star Schema     |
| Langage         | SQL             |

## Bonnes pratiques

- ✅ Clés primaires sur toutes les dimensions
- ✅ Clés étrangères référentielles
- ✅ Index sur les colonnes de jointure
- ✅ `ON CONFLICT DO NOTHING` pour l'idempotence
- ✅ Séparation staging / DW

## Statut

✅ **Terminé et validé**

---

**Auteur :** MAAOUIA Ahmed – CY Tech Big Data
