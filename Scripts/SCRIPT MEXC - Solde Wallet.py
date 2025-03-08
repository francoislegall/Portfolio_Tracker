# IMPORT DES BIBLIOTHÈQUES
import json
import pandas as pd
import requests
from datetime import date, timedelta
import time
import hmac
import hashlib
from dotenv import load_dotenv
import os

# --------------------------------------------------------------------------------

# PARAMÉTRAGE DE L'API

# Charger les variables d'environnement depuis un fichier .env
load_dotenv()

# Récupérer les clés API MEXC depuis les variables d'environnement
api_key = os.getenv("mexc_api_key")
secret_key = os.getenv("mexc_secret_key")

# Définir l'endpoint de l'API MEXC
base_url = "https://api.mexc.com"
endpoint = "/api/v3/account"

# Générer le timestamp actuel en millisecondes
timestamp = int(time.time() * 1000)

# Construire la chaîne de requête avec le timestamp
query_string = f"timestamp={timestamp}"

# Générer la signature HMAC SHA256 pour authentifier la requête
signature = hmac.new(secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

# Construire l'URL finale avec la signature
url = f"{base_url}{endpoint}?{query_string}&signature={signature}"

# Définir les en-têtes de la requête avec la clé API
headers = {
    "X-MEXC-APIKEY": api_key
}

# Envoyer la requête GET à l'API MEXC
response = requests.get(url, headers=headers)

# Vérifier la réponse et afficher le solde du portefeuille si la requête est réussie
if response.status_code == 200:
    data = response.json()
    print("Solde du portefeuille Spot :", data["balances"])
else:
    print(f"Erreur : {response.status_code}, {response.text}")

# --------------------------------------------------------------------------------

# TRANSFORMATION DES DONNÉES

# Convertir la réponse JSON en un DataFrame pandas
df_wallet = pd.DataFrame(data["balances"])

# Supprimer la colonne 'locked' qui n'est pas nécessaire
df_wallet = df_wallet.drop(columns=['locked'])

# Ajouter une colonne avec la date actuelle
df_wallet['date'] = date.today()

# Ajouter une colonne indiquant la plateforme
df_wallet['plateforme'] = 'MEXC'

# Ajouter une colonne pour le protocole, initialisée à None
df_wallet['protocole'] = None

# Ajouter une colonne pour le type de position, ici 'wallet'
df_wallet['type_position'] = 'wallet'

# Ajouter une colonne pour l'adresse, initialisée à None
df_wallet['adresse'] = None

# Renommer les colonnes pour plus de clarté
df_wallet.rename(columns={'asset': 'symbol', 'free': 'montant'}, inplace=True)

# Convertir la colonne 'montant' en type float
df_wallet['montant'] = df_wallet['montant'].astype(float)

# Réorganiser les colonnes
df_wallet = df_wallet.reindex(['date', 'symbol', 'plateforme', 'montant', 'type_position', 'protocole', 'adresse'], axis=1)

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

# Insérer le DataFrame dans une table MySQL existante, en ajoutant les nouvelles données sans supprimer les anciennes
df_wallet.to_sql("mexc_soldewallet", con=engine, if_exists="append", index=False)