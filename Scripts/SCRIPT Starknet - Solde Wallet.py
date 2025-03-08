# IMPORT DES BIBLIOTHÈQUES
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from datetime import date, timedelta
from sqlalchemy import create_engine
import pymysql
from dotenv import load_dotenv
import os

# --------------------------------------------------------------------------------

# PARAMÉTRAGE DE L'ENVIRONNEMENT

# Charger les variables d'environnement depuis un fichier .env
load_dotenv()

# Récupérer les adresses des portefeuilles depuis les variables d'environnement
Argent = os.getenv("argent_adress")
Braavos = os.getenv("braavos_adress")

# --------------------------------------------------------------------------------

# CONFIGURATION DE SELENIUM POUR ARGENT

# Configurer le driver Selenium pour une exécution sans interface graphique
options = webdriver.ChromeOptions()
options.add_argument('--headless')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Charger la page cible pour le portefeuille Argent
url = f"https://portfolio.argent.xyz/overview/{Argent}"
driver.get(url)

# Attendre le chargement complet de la page
driver.implicitly_wait(10)

# Localiser tous les conteneurs de jetons sur la page
tokens = driver.find_elements(By.CLASS_NAME, "css-x01ui3")

# Initialiser les listes pour stocker les symboles et les montants des jetons
symbols = []
amounts = []

# Extraire le montant et le symbole pour chaque jeton
for token in tokens:
    try:
        amount = token.find_element(By.CLASS_NAME, "css-1ac2ftb").text
        symbol = token.find_elements(By.TAG_NAME, "p")[1].text
        amounts.append(amount)
        symbols.append(symbol)
    except Exception as e:
        print(f"Erreur pour un jeton : {e}")

# Fermer le driver Selenium
driver.quit()

# Afficher les listes récupérées
print("Liste des symboles :", symbols)
print("Liste des montants :", amounts)

# Créer un DataFrame à partir des données extraites
df_argent = pd.DataFrame({'symbol': symbols, 'montant': amounts})

# Remplacer les virgules par des points et convertir les montants en float
df_argent['montant'] = df_argent['montant'].str.replace(',', '').astype(float)

# Ajouter une colonne avec l'adresse du portefeuille
df_argent['adresse'] = Argent

# --------------------------------------------------------------------------------

# CONFIGURATION DE SELENIUM POUR BRAAVOS

# Configurer le driver Selenium pour une exécution sans interface graphique
options = webdriver.ChromeOptions()
options.add_argument('--headless')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Charger la page cible pour le portefeuille Braavos
url = f"https://portfolio.argent.xyz/overview/{Braavos}"
driver.get(url)

# Attendre le chargement complet de la page
driver.implicitly_wait(10)

# Localiser tous les conteneurs de jetons sur la page
tokens = driver.find_elements(By.CLASS_NAME, "css-x01ui3")

# Initialiser les listes pour stocker les symboles et les montants des jetons
symbols = []
amounts = []

# Extraire le montant et le symbole pour chaque jeton
for token in tokens:
    try:
        amount = token.find_element(By.CLASS_NAME, "css-1ac2ftb").text
        symbol = token.find_elements(By.TAG_NAME, "p")[1].text
        amounts.append(amount)
        symbols.append(symbol)
    except Exception as e:
        print(f"Erreur pour un jeton : {e}")

# Fermer le driver Selenium
driver.quit()

# Afficher les listes récupérées
print("Liste des symboles :", symbols)
print("Liste des montants :", amounts)

# Créer un DataFrame à partir des données extraites
df_braavos = pd.DataFrame({'symbol': symbols, 'montant': amounts})

# Remplacer les virgules par des points et convertir les montants en float
df_braavos['montant'] = df_braavos['montant'].str.replace(',', '').astype(float)

# Ajouter une colonne avec l'adresse du portefeuille
df_braavos['adresse'] = Braavos

# --------------------------------------------------------------------------------

# FUSION ET TRANSFORMATION DES DONNÉES

# Fusionner les DataFrames des deux portefeuilles
df_argent_braavos = pd.concat([df_argent, df_braavos], axis=0)

# Ajouter une colonne avec la date actuelle
df_argent_braavos['date'] = date.today()

# Ajouter une colonne indiquant la plateforme (StarkNet)
df_argent_braavos['plateforme'] = 'starknet'

# Initialiser la colonne 'protocole' avec une chaîne vide
df_argent_braavos['protocole'] = ''

# Fonction pour déterminer le type de position en fonction du symbole et du montant
def check_condition(row):
    symbol = row['symbol']
    montant = row['montant']
    if symbol.endswith('STRK') and any(c.islower() for c in symbol[:-4]):
        return 'staked'
    elif symbol.endswith('ETH') and any(c.islower() for c in symbol[:-3]):
        return 'deposit'
    elif montant < 0:
        return 'loan'
    return 'wallet'

# Appliquer la fonction pour créer la colonne 'type_position'
df_argent_braavos['type_position'] = df_argent_braavos.apply(check_condition, axis=1)

# Réorganiser les colonnes pour un format cohérent
df_argent_braavos = df_argent_braavos.reindex(['date', 'symbol', 'plateforme', 'montant', 'type_position', 'protocole', 'adresse'], axis=1)

# --------------------------------------------------------------------------------

# PRÉPARATION DES DONNÉES POUR LA VISUALISATION

# Dupliquer le DataFrame pour créer une version adaptée à la visualisation
df_argent_braavos_dataviz = df_argent_braavos.copy()

# Créer une colonne 'symbol', suppimer les lettres minuscules (ex: ezETH = ETH) pour la jointure avec une table de prix
df_argent_braavos_dataviz['symbol'] = df_argent_braavos_dataviz['symbol'].apply(lambda x: ''.join([char for char in x if not char.islower()]))

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

# Insérer les données dans la table 'starknet_soldewallet'
df_argent_braavos.to_sql("starknet_soldewallet", con=engine, if_exists="append", index=False)

# Insérer les données adaptées pour la visualisation dans une autre table
df_argent_braavos_dataviz.to_sql("starknet_soldewallet_dataviz", con=engine, if_exists="append", index=False)