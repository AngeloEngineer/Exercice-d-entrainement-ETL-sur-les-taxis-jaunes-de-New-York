import pandas as pd
from sqlalchemy import create_engine
import glob
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# Vérifier que toutes les variables sont définies
if not all([db_user, db_password, db_host, db_port, db_name]):
    raise ValueError("Les variables d'environnement DB_USER, DB_PASSWORD, DB_HOST, DB_PORT et DB_NAME doivent être définies dans un fichier .env")

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

def transform_data(df):
    """
    Fonction de nettoyage et transformation des données.
    """
    # Conversion des colonnes de date au bon format
    if 'tpep_pickup_datetime' in df.columns:
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    moy_passenger_count = int(df['passenger_count'].mean())
    df['passenger_count'] = df['passenger_count'].fillna(moy_passenger_count)
    df['RatecodeID'] = df['RatecodeID'].fillna(df['RatecodeID'].mode())
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna(df['store_and_fwd_flag'].mode())

    # Ne considerer que les taxis qui ont transporté des passagers et les taxis qui ont éffectués une certaine distance
    if 'passenger_count' in df.columns and 'trip_distance' in df.columns:
        df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]
    
    # Remplissage des valeurs nulles pour certaines colonnes financières
    cols_to_fill = ['congestion_surcharge', 'Airport_fee']
    for col in cols_to_fill:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    # Moyenne uniquement sur les valeurs positives
    if 'Airport_fee' in df.columns:
        moyenne_reelle_airport_fee = df.loc[df['Airport_fee'] > 0, 'Airport_fee'].mean()
        df.loc[df['Airport_fee'] < 0, 'Airport_fee'] = moyenne_reelle_airport_fee
    if 'congestion_surcharge' in df.columns:
        moyenne_reelle_congestion_surcharge = df.loc[df['congestion_surcharge'] > 0, 'congestion_surcharge'].mean()
        df.loc[df['congestion_surcharge'] < 0, 'congestion_surcharge'] = moyenne_reelle_congestion_surcharge
    return df

def load_data(df, table_name, engine):
    """
    Chargement des données dans PostgreSQL.
    """
    # 'append' permet d'ajouter les données à la table existante
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize=100000)
    print(f"{len(df)} lignes insérées dans la table {table_name}.")

def main():
    '''
    
    GESTION DE MULTIPLES FICHIERS EN BATCH (JANVIER, FÉVRIER, MARS SIMULTANÉMENT)
    
    Pour traiter plusieurs mois en même temps ou à la suite, l'approche idéale 
    est d'utiliser le module `glob` pour lister dynamiquement tous les fichiers 
    Parquet dans un répertoire de dépôt.
    
    Au lieu de coder en dur "yellow_tripdata_2025-01.parquet", tu mets tes 
    fichiers de Janvier, Février et Mars dans un dossier (ex: ./data_source/).
    
    Le code ci-dessous (la boucle for) va itérer sur chaque fichier, l'extraire, 
    le transformer en mémoire, et l'ajouter (append) à la même table PostgreSQL.
    
    Si les fichiers sont massifs (plusieurs Go chacun), cette boucle lit 
    séquentiellement, ce qui évite de saturer la RAM de ta machine.
    
    Pour une exécution VRAIMENT simultanée (parallèle), tu pourrais utiliser 
    `concurrent.futures.ThreadPoolExecutor` ou `ProcessPoolExecutor` pour 
    lancer l'ETL sur plusieurs fichiers en parallèle.
    
    '''
    
    # Chemin vers le dossier contenant tes fichiers Parquet
    data_dir = '/home/angelo-btma/ETL parquet/' 
    fichiers_parquet = glob.glob(os.path.join(data_dir, "yellow_tripdata_2025-*.parquet"))
    
    if not fichiers_parquet:
        print("Aucun fichier trouvé.")
        return

    table_destination = 'yellow_taxi_trips_2025'

    # Boucle de traitement Batch
    for fichier in sorted(fichiers_parquet):
        print(f"Début du traitement pour : {os.path.basename(fichier)}")
        
        # 1. EXTRACT
        # pandas lit nativement le parquet grâce à pyarrow ou fastparquet
        df = pd.read_parquet(fichier)
        
        # 2. TRANSFORM
        df_clean = transform_data(df)
        
        # 3. LOAD
        load_data(df_clean, table_destination, engine)
        
        print(f"Fichier {os.path.basename(fichier)} terminé.\n")


if __name__ == "__main__":
    main()