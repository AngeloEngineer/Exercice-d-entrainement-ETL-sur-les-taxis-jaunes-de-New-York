# README - ETL des trajets de taxis jaunes (New York, 2025)

Ce projet implémente un pipeline ETL (Extract, Transform, Load) pour les données des trajets de taxis jaunes de New York des trois premiers mois de 2025 (janvier, février, mars). Les fichiers sources sont au format Parquet et sont chargés dans une base de données PostgreSQL.

## Source des données

Les fichiers utilisés :
- `yellow_tripdata_2025-01.parquet`
- `yellow_tripdata_2025-02.parquet`
- `yellow_tripdata_2025-03.parquet`

Ils sont disponibles aux adresses fournies par la TLC : `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page` et ont été placés dans mon répertoire `/home/angelo-btma/ETL parquet/`.

## Description du pipeline

Le script `etl_parquet.py` exécute les étapes suivantes :

### 1. Extraction
- Lecture de chaque fichier Parquet à l’aide de `pandas.read_parquet()`
- Traitement séquentiel des fichiers (boucle `for`)

### 2. Transformation
Les transformations appliquées sont :
- Conversion des colonnes `tpep_pickup_datetime` et `tpep_dropoff_datetime` en type datetime
- Remplissage des valeurs manquantes :
  - `passenger_count` : par la moyenne (convertie en entier) des valeurs présentes
  - `RatecodeID` : par le mode (valeur la plus fréquente)
  - `store_and_fwd_flag` : par le mode
  - `congestion_surcharge` et `Airport_fee` : par 0 (zéro)
- Filtrage : seuls les trajets avec `passenger_count > 0` et `trip_distance > 0` sont conservés
- Correction des valeurs négatives :
  - Pour `Airport_fee` et `congestion_surcharge`, les valeurs strictement négatives sont remplacées par la moyenne des valeurs strictement positives de la même colonne

### 3. Chargement
- Connexion à PostgreSQL via SQLAlchemy en utilisant des variables d’environnement (fichier `.env`)
- Insertion des données dans la table `yellow_taxi_trips_2025` en mode `append` (ajout incrémental)
- Insertion par lots de 100 000 lignes

## Configuration requise

- Python 3.x avec les paquets : pandas, sqlalchemy, psycopg2, python-dotenv, pyarrow (ou fastparquet)
- Base de données PostgreSQL
- Fichier `.env` contenant :
  ```
  DB_USER=
  DB_PASSWORD=
  DB_HOST=
  DB_PORT=
  DB_NAME=
  ```

## Exécution

```bash
python etl_parquet.py
```

Les fichiers Parquet doivent être présents dans le répertoire indiqué. Le script traite chaque fichier un par un et affiche le nombre de lignes insérées.

## Limites et points faibles du travail actuel

1. **Gestion incomplète des colonnes** : Le script ne traite pas toutes les colonnes présentes dans le dictionnaire de données (par exemple `cbd_congestion_fee`, `airport_fee` en minuscule alors que le code utilise `Airport_fee`). Une incohérence de casse peut causer des erreurs ou des omissions.
2. **Absence de journalisation et de gestion d’erreurs** : En cas d’échec sur un fichier (mauvais format, connexion à la base interrompue), le script s’arrête sans reprise ni journal. Les erreurs potentielles (ex. `mode()` retournant plusieurs valeurs) ne sont pas gérées.
3. **Traitement séquentiel sans parallélisme** : Les fichiers sont lus un après l’autre, ce qui n’exploite pas les possibilités de chargement parallèle pour de gros volumes de données.

## Points forts

1. **Utilisation de variables d’environnement** : Les identifiants de connexion à PostgreSQL sont externalisés, bonne pratique de sécurité.
2. **Nettoyage actif des données** : Remplissage des valeurs nulles, suppression des trajets invalides (distance ou passagers nuls), correction des valeurs aberrantes négatives.
3. **Traitement par lots (batch)** : Le script liste dynamiquement tous les fichiers Parquet correspondant à un motif et les traite séquentiellement, évitant de surcharger la mémoire.

## Pistes d’amélioration pour progresser dans les pipelines de données

1. **Introduire un framework d’orchestration** : Utiliser Apache Airflow, Dagster ou Prefect pour planifier l’exécution, gérer les dépendances, la reprise sur échec et la surveillance.
2. **Passer à un modèle ELT (chargement puis transformation)** : Charger les fichiers bruts dans une table staging PostgreSQL (ou un lac comme S3), puis appliquer les transformations directement en SQL (avec dbt par exemple). Cela sépare mieux les responsabilités.
3. **Autres jeux de données pour s’entraîner** :
   - Données de ventes e-commerce (fichiers CSV/JSON de transactions, mise en place d’un schéma en étoile)
   - Logs d’applications web (parsing, agrégations temporelles, détection d’anomalies)
   - Données météorologiques horaires (API OpenWeatherMap ou NOAA) avec chargement incrémental et gestion des séries temporelles
   - Flux de clics (clickstream) depuis des fichiers Parquet ou Avro pour pratiquer l’optimisation des jointures et du partitionnement
