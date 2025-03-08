# IMPORT DES BIBLIOTHÈQUES
import pymysql
import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
import os

# Charger les variables d'environnement depuis un fichier .env
load_dotenv()

# --------------------------------------------------------------------------------

# RÉCUPÉRATION DE LA LISTE DES SYMBOLS

# URL de l'API CoinGecko pour obtenir la liste des jetons et leurs symboles
url = "https://api.coingecko.com/api/v3/coins/list"

# Faire la requête à l'API
response2 = requests.get(url)

# Vérifier la réponse et afficher un message d'erreur si nécessaire
if str(response2) == "<Response [429]>":
    print("Erreur 429 (Trop de requêtes d'un coup)")
else:
    print("Tout est OK mon reuf")

# Convertir la réponse JSON en une liste de jetons
coins = response2.json()

# Convertir la liste en DataFrame
df_symbol = pd.DataFrame(coins)

# Convertir les symboles en majuscules
df_symbol['symbol'] = df_symbol['symbol'].str.upper()

# --------------------------------------------------------------------------------

# LISTE DES CRYPTOS DONT ON SOUHAITE RÉCUPÉRER LE PRIX

Liste = [
    "cardano", "aixbt", "aixcb-by-virtuals", "ankr", "arbitrum", "binancecoin",
    "bitcoin", "elrond-erd-2", "ethereum", "fetch-ai", "injective-protocol",
    "io", "chainlink", "memes-ai", "mantle", "near", "ondo-finance", "optimism",
    "reserve-rights-token", "solana", "starknet", "bittensor", "usd-coin",
    "tether", "virtual-protocol", "wrapped-bitcoin", "weth", "woo-network",
    "wrapped-steth", "pippin", "susd-optimism", "venice-token", "havven"
]

# --------------------------------------------------------------------------------

# FONCTION POUR TRANSFORMER LES DONNÉES EN DATAFRAME

# Fonction pour transformer les données JSON en DataFrame
def create_df(crypto_data, crypto_name):
    prices = pd.DataFrame(crypto_data['prices'], columns=['timestamp', 'price'])
    market_caps = pd.DataFrame(crypto_data['market_caps'], columns=['timestamp', 'market_cap'])
    total_volumes = pd.DataFrame(crypto_data['total_volumes'], columns=['timestamp', 'total_volume'])

    # Fusionner les DataFrames sur le timestamp
    df = pd.merge(prices, market_caps, on='timestamp', how='outer')
    df = pd.merge(df, total_volumes, on='timestamp', how='outer')

    # Ajouter une colonne pour le nom de la cryptomonnaie
    df['crypto'] = crypto_name

    # Convertir le timestamp en date
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms')

    return df

# --------------------------------------------------------------------------------

# APPEL À L'API POUR RÉCUPÉRER LES DONNÉES DE MARCHÉ

# Définir les paramètres pour l'appel à l'API
currency = 'usd'
days_before = 1
interval = 'daily'

headers = {
    "accept": "application/json",
    "x-cg-demo-api-key": os.getenv("coingecko_api_key")
}

# Liste pour stocker les données récupérées
all_data = []

# Boucle pour récupérer les données de marché pour chaque crypto
for crypto in Liste:
    url = f"https://api.coingecko.com/api/v3/coins/{crypto}/market_chart?vs_currency={currency}&days={days_before}&interval={interval}"
    response = requests.get(url, headers=headers)
    data = response.json()

    if response.status_code == 200:
        print(f"{crypto}: OK")
        df_crypto = create_df(data, crypto)
        all_data.append(df_crypto)
    else:
        print(f"{crypto}: ERREUR {response.status_code}")

    time.sleep(2)  # Délai de 2s entre les requêtes pour respecter les limites de l'API

# Fusionner tous les DataFrames dans all_data en un seul DataFrame
final_df = pd.concat(all_data, ignore_index=True)

# --------------------------------------------------------------------------------

# TRANSFORMATION DES DONNÉES

# Convertir la colonne 'date' en type datetime
final_df['date'] = pd.to_datetime(final_df['date'])

# Garder uniquement la date (sans l'heure)
final_df['date'] = final_df['date'].dt.date

# Supprimer la colonne 'timestamp'
final_df.drop(columns=["timestamp"], inplace=True)

# Réorganiser les colonnes
final_df = final_df.reindex(['date', 'price', 'market_cap', 'total_volume', 'crypto'], axis=1)

# Filtrer le DataFrame pour ne garder que les indices pairs (prix d'ouvertures)
filtered_df = final_df.iloc[::2].reset_index(drop=True)

# Jointure pour récupérer le symbole sur la table
df_price_symbol = pd.merge(filtered_df, df_symbol, left_on=['crypto'], right_on=['id'])

# Supprimer les colonnes inutiles
df_price_symbol = df_price_symbol.drop(['id', 'crypto', 'name'], axis=1)

# Réorganiser les colonnes
df_price_symbol = df_price_symbol.reindex(['date', 'symbol', 'price', 'market_cap', 'total_volume'], axis=1)

# Renommer les colonnes
df_price_symbol = df_price_symbol.rename(columns={'price': 'prix'})

# --------------------------------------------------------------------------------

# ENREGISTREMENT DES DONNÉES DANS MYSQL

# Récupérer les informations de connexion MySQL depuis les variables d'environnement
db_username = os.getenv("db_username")
db_password = os.getenv("db_password")
db_name = os.getenv("db_name")
db_host = os.getenv("db_host")
db_port = os.getenv("db_port")

# Créer un moteur de connexion MySQL avec SQLAlchemy
engine = create_engine(f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

# Insérer le DataFrame dans une table MySQL existante, en ajoutant les nouvelles données sans supprimer les anciennes
df_price_symbol.to_sql("crypto_price", con=engine, if_exists="append", index=False)