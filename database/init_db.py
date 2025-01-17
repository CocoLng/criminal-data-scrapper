import logging

import mysql.connector
from mysql.connector import Error

from .data_loader import DataLoader
from .db_config import DatabaseConfig

logger = logging.getLogger(__name__)


class DatabaseInitializer:
    def __init__(self):
        self.config = DatabaseConfig()
        self.data_loader = DataLoader()

    def _check_data_exists(self, cursor) -> bool:
        """Vérifie si des données existent déjà dans la base"""
        try:
            cursor.execute("SELECT COUNT(*) as count FROM statistiques")
            result = cursor.fetchone()
            return result[0] > 0
        except Error:
            return False

    def _initialize_tables(self, cursor) -> None:
        """Initialise toutes les tables de la base de données"""
        for table_name, table_description in self.config.TABLES.items():
            try:
                logger.info(f"Création de la table {table_name}")
                cursor.execute(table_description)
            except Error as e:
                logger.error(
                    f"Erreur lors de la création de la table {table_name}: {e}"
                )
                raise

    def create_database(self, force_reload: bool = False) -> None:
        """Crée la base de données et charge les données initiales"""
        try:
            # Connexion sans sélectionner de base de données
            conn = mysql.connector.connect(
                host=self.config.HOST,
                user=self.config.USER,
                password=self.config.PASSWORD,
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

            # Initialisation des tables
            self._initialize_tables(cursor)

            # Vérifier si les données existent déjà
            data_exists = self._check_data_exists(cursor)

            if not data_exists or force_reload:
                # Chargement des données initiales avec Pandas et DataLoader
                try:
                    logger.info("Chargement des données initiales...")
                    self.data_loader.load_data(
                        source="csv",
                        file_path="donnee-del-data.gouv.csv",
                    )
                    logger.info("Données initiales chargées avec succès")
                except Exception as e:
                    logger.error(
                        f"Erreur lors du chargement des données initiales: {e}"
                    )
                    raise
            else:
                logger.info("Les données existent déjà dans la base")

        except Error as e:
            logger.error(f"Erreur lors de la connexion à MySQL: {e}")
            raise
        finally:
            if "conn" in locals() and conn.is_connected():
                cursor.close()
                conn.close()
