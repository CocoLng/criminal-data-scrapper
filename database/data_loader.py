import pandas as pd
import mysql.connector
import logging
import requests
from mysql.connector import Error
from .db_config import DatabaseConfig
from typing import Optional, Dict, Any
import json

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self):
        self.config = DatabaseConfig()
        self.API_BASE_URL = "https://www.data.gouv.fr/api/1"
        self.DATASET_ID = "base-statistique-communale-departementale-et-regionale-de-la-delinquance-enregistree"
    
    def fetch_from_api(self) -> Optional[pd.DataFrame]:
        """Récupère les données depuis l'API data.gouv.fr"""
        try:
            # Récupération des métadonnées du dataset
            dataset_url = f"{self.API_BASE_URL}/datasets/{self.DATASET_ID}/"
            response = requests.get(dataset_url)
            response.raise_for_status()
            
            dataset_info = response.json()
            logger.info(f"Dataset trouvé: {dataset_info['title']}")
            
            # Récupération de l'URL de la ressource la plus récente
            resources = dataset_info.get('resources', [])
            if not resources:
                raise ValueError("Aucune ressource trouvée dans le dataset")
            
            # Trier les ressources par date et prendre la plus récente
            latest_resource = sorted(
                resources,
                key=lambda x: x['last_modified'],
                reverse=True
            )[0]
            
            # Téléchargement des données
            data_response = requests.get(latest_resource['url'])
            data_response.raise_for_status()
            
            # Conversion en DataFrame
            df = pd.read_csv(data_response.content, sep=';', encoding='utf-8')
            logger.info(f"Données téléchargées avec succès: {len(df)} lignes")
            
            return self._clean_data(df)
            
        except requests.RequestException as e:
            logger.error(f"Erreur lors de la requête API: {e}")
            raise
        except Exception as e:
            logger.error(f"Erreur lors du téléchargement des données: {e}")
            raise
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Nettoie et prépare les données"""
        try:
            # Nettoyage des noms de colonnes
            df.columns = df.columns.str.lower().str.replace('.', '_')
            
            # Conversion des types de données
            numeric_columns = ['annee', 'faits', 'pop', 'tauxpourmille']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Suppression des lignes avec des valeurs manquantes
            df = df.dropna(subset=['code_region', 'faits', 'pop'])
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors du nettoyage des données: {e}")
            raise
    
    def load_data(self, source: str = 'api', file_path: Optional[str] = None) -> None:
        """
        Charge les données depuis l'API ou un fichier CSV
        
        Args:
            source: 'api' ou 'csv'
            file_path: Chemin du fichier CSV si source='csv'
        """
        try:
            # Récupération des données
            if source == 'api':
                df = self.fetch_from_api()
            elif source == 'csv' and file_path:
                df = pd.read_csv(file_path, sep=';', encoding='utf-8')
                df = self._clean_data(df)
            else:
                raise ValueError("Source non valide ou fichier manquant")
            
            # Insertion dans la base de données
            self.insert_data(df)
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement des données: {e}")
            raise
    
    def insert_data(self, df: pd.DataFrame) -> None:
        """Insère les données dans la base de données"""
        try:
            conn = mysql.connector.connect(**self.config.get_connection_params())
            cursor = conn.cursor()
            
            # Désactivation temporaire des contraintes de clé étrangère
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # Insertion des régions uniques
            regions = df[['code_region', 'region']].drop_duplicates()
            for _, row in regions.iterrows():
                cursor.execute("""
                    INSERT IGNORE INTO regions (code_region, nom_region)
                    VALUES (%s, %s)
                """, (row['code_region'], row['region']))
            
            # Insertion des catégories uniques avec leurs unités de compte
            categories = df[['classe', 'unite_de_compte']].drop_duplicates()
            for _, row in categories.iterrows():
                cursor.execute("""
                    INSERT IGNORE INTO categories (classe, unite_compte)
                    VALUES (%s, %s)
                """, (row['classe'], row['unite_de_compte']))
            
            # Insertion des statistiques
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO statistiques 
                    (code_region, classe, annee, unite_compte, faits, 
                     population, logements, taux_pour_mille)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['code_region'],
                    row['classe'],
                    row['annee'],
                    row['unite_de_compte'],
                    row['faits'],
                    row['pop'],
                    row['log'],
                    row['tauxpourmille']
                ))
            
            # Réactivation des contraintes de clé étrangère
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            conn.commit()
            logger.info("Données insérées avec succès")
            
        except Error as e:
            logger.error(f"Erreur lors de l'insertion des données: {e}")
            conn.rollback()
            raise
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()