# IMPORT DES BIBLIOTHÈQUES
import pandas as pd
from binance.client import Client
from datetime import date, timedelta
from dotenv import load_dotenv
import os

# --------------------------------------------------------------------------------

# PARAMÉTRAGE DE L'API

# Charger les variables depuis .env
load_dotenv()

# Récupération des clés API depuis les variables d'environnement
api_key = os.getenv("binance_api_key")
api_secret = os.getenv("binance_api_secret")

# Initialisation du client Binance
client = Client(api_key, api_secret)

# Récupération des informations du compte
account = client.get_account()

# --------------------------------------------------------------------------------

# NETTOYAGE ET PRÉPARATION DES DONNÉES

# Récupération des montants
df_wallet = pd.DataFrame(data=account["balances"])

# Suppression de la colonne "locked"
df_wallet = df_wallet.drop(['locked'], axis=1)

# Renommer les colonnes
df_wallet.rename(columns={'free': 'montant', 'asset': 'symbol'}, inplace=True)

# Modifier le type de la colonne en float
df_wallet['montant'] = df_wallet['montant'].astype(float)

# Ne pas afficher les valeurs < 0
df_wallet = df_wallet[df_wallet['montant'] > 0]

# Ajouter une colonne date avec la date du jour
df_wallet['date'] = date.today()

# Ajouter une colonne plateforme
df_wallet['plateforme'] = 'Binance'

# Ajouter une colonne protocole
df_wallet['protocole'] = None

# Ajouter une colonne type_position
df_wallet['type_position'] = 'wallet'

# Ajouter une colonne adresse
df_wallet['adresse'] = None

# Réorganiser les colonnes
df_wallet = df_wallet.reindex(['date', 'symbol', 'plateforme', 'montant', 'type_position', 'protocole', 'adresse'], axis=1)

# Supprimer "LD" si cela apparaît au début des symbols
df_wallet['symbol'] = df_wallet['symbol'].str.replace('^LD', '', regex=True)

# --------------------------------------------------------------------------------

# ENREGISTREMENT DES DONNÉES DANS MYSQL

# Import des bibliothèques pour la connexion à la base de données
import pymysql
from sqlalchemy import create_engine

# Récupération des informations de connexions MySQL depuis les variables d'environnement
db_username = os.getenv("db_username")
db_password = os.getenv("db_password")
db_name = os.getenv("db_name")
db_host = os.getenv("db_host")
db_port = os.getenv("db_port")

# Création d'un moteur MySQL avec sqlalchemy
engine = create_engine(f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

# Insertion dans la table existante (mode append pour ajouter sans supprimer les anciennes données)
df_wallet.to_sql("binance_soldewallet", con=engine, if_exists="append", index=False)