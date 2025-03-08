# IMPORT DES BIBLIOTHÈQUES
import json
import pandas as pd
import requests
from datetime import date, timedelta
from dotenv import load_dotenv
import os

# --------------------------------------------------------------------------------

# PARAMÉTRAGE DE L'API

# Charger les variables d'environnement depuis un fichier .env
load_dotenv()

# Récupérer l'adresse du portefeuille depuis les variables d'environnement
address = os.getenv("evm_adress")

# Configurer l'URL de l'API et les en-têtes pour la requête
url = f"https://api.zerion.io/v1/wallets/{address}/positions/?filter[positions]=no_filter&currency=usd&filter[trash]=only_non_trash&sort=value"
headers = {
    "accept": "application/json",
    "authorization": f"Basic {os.getenv('zerion_api_key')}"
}

# Effectuer la requête GET à l'API pour récupérer les données du portefeuille
response = requests.get(url, headers=headers)

# Convertir la réponse JSON en un dictionnaire Python
json_data = response.json()

# Normaliser les données JSON pour les convertir en un DataFrame pandas
df_wallet = pd.json_normalize(json_data["data"])

# --------------------------------------------------------------------------------

# NETTOYAGE DES DONNÉES

# Sélectionner uniquement les colonnes nécessaires
df_EVM = df_wallet[['relationships.chain.data.id', 'attributes.protocol',
                    'attributes.fungible_info.symbol', 'attributes.quantity.numeric',
                    'attributes.position_type']].copy()

# Renommer les colonnes pour plus de clarté
df_EVM.rename(columns={
    'relationships.chain.data.id': 'plateforme',
    'attributes.protocol': 'protocole',
    'attributes.fungible_info.symbol': 'symbol',
    'attributes.quantity.numeric': 'montant',
    'attributes.position_type': 'type_position'
}, inplace=True)

# Ajouter une colonne avec la date actuelle
df_EVM['date'] = date.today()

# Ajouter une colonne contenant l'adresse du portefeuille
df_EVM['adresse'] = address

# Convertir la colonne 'montant' en type float
df_EVM['montant'] = df_EVM['montant'].astype(float)

# Mettre à jour les montants des positions de type 'loan' en négatif
df_EVM.loc[df_EVM['type_position'] == 'loan', 'montant'] *= -1

# Réorganiser les colonnes
df_EVM = df_EVM.reindex(['date', 'symbol', 'plateforme', 'montant', 'type_position', 'protocole', 'adresse'], axis=1)

# --------------------------------------------------------------------------------

# ENREGISTREMENT DES DONNÉES DANS MYSQL

# Import des bibliothèques nécessaires pour la connexion à la base de données
import pymysql
from sqlalchemy import create_engine

# Récupérer les informations de connexion MySQL depuis les variables d'environnement
db_username = os.getenv("db_username")
db_password = os.getenv("db_password")
db_name = os.getenv("db_name")
db_host = os.getenv("db_host")
db_port = os.getenv("db_port")

# Créer un moteur de connexion MySQL avec SQLAlchemy
engine = create_engine(f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

# Écrire le DataFrame dans une table MySQL existante, en ajoutant les nouvelles données sans supprimer les anciennes
df_EVM.to_sql("evm_soldewallet", con=engine, if_exists="append", index=False)