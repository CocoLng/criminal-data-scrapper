import mysql.connector
import logging
from mysql.connector import Error
from .db_config import DatabaseConfig

logger = logging.getLogger(__name__)

class DatabaseInitializer:
    def __init__(self):
        self.config = DatabaseConfig()
    
    def create_database(self):
        """Crée la base de données et les tables nécessaires"""
        try:
            # Connexion sans sélectionner de base de données
            conn = mysql.connector.connect(
                host=self.config.HOST,
                user=self.config.USER,
                password=self.config.PASSWORD
            )
            cursor = conn.cursor()
            
            # Création de la base de données
            try:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.config.DATABASE}")
                logger.info(f"Base de données {self.config.DATABASE} créée avec succès")
            except Error as e:
                logger.error(f"Erreur lors de la création de la base de données: {e}")
                raise
            
            # Sélection de la base de données
            cursor.execute(f"USE {self.config.DATABASE}")
            
            # Création des tables
            for table_name, table_description in self.config.TABLES.items():
                try:
                    logger.info(f"Création de la table {table_name}")
                    cursor.execute(table_description)
                except Error as e:
                    logger.error(f"Erreur lors de la création de la table {table_name}: {e}")
                    raise
            
            logger.info("Toutes les tables ont été créées avec succès")
            
        except Error as e:
            logger.error(f"Erreur lors de la connexion à MySQL: {e}")
            raise
        
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
                logger.info("Connexion MySQL fermée")
    
    def clean_database(self):
        """Nettoie la base de données en supprimant toutes les données"""
        try:
            conn = mysql.connector.connect(**self.config.get_connection_params())
            cursor = conn.cursor()
            
            # Désactiver temporairement les contraintes de clé étrangère
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # Supprimer les données de toutes les tables
            for table_name in self.config.TABLES.keys():
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                logger.info(f"Table {table_name} vidée avec succès")
            
            # Réactiver les contraintes de clé étrangère
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            conn.commit()
            logger.info("Nettoyage de la base de données terminé")
            
        except Error as e:
            logger.error(f"Erreur lors du nettoyage de la base de données: {e}")
            raise
        
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()