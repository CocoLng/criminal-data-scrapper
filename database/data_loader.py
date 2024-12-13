import logging
from typing import Optional

import mysql.connector
import numpy as np
import pandas as pd
from mysql.connector import Error
from sqlalchemy import create_engine

from .db_config import DatabaseConfig

logger = logging.getLogger(__name__)


class DataLoader:
    def __init__(self):
        self.config = DatabaseConfig()
        # Connection SQLAlchemy pour les opérations Pandas
        self.engine = create_engine(
            f"mysql+mysqlconnector://{self.config.USER}:{self.config.PASSWORD}@{self.config.HOST}/{self.config.DATABASE}"
        )

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Nettoie et prépare les données avec Pandas"""
        try:
            # Mapping des colonnes
            column_mapping = {
                "Code.région": "code_region",
                "classe": "classe",
                "annee": "annee",
                "unité.de.compte": "unite_compte",
                "faits": "faits",
                "POP": "population",
                "LOG": "logements",
                "tauxpourmille": "taux_pour_mille",
            }

            # Renommage des colonnes
            df = df.rename(columns=column_mapping)

            # Nettoyage des strings
            string_columns = ["code_region", "classe", "unite_compte"]
            df[string_columns] = (
                df[string_columns].astype(str).apply(lambda x: x.str.strip())
            )

            # Remplacer les virgules par des points dans les colonnes numériques
            numeric_columns = ["faits", "population", "logements", "taux_pour_mille"]
            for col in numeric_columns:
                df[col] = df[col].astype(str).str.replace(",", ".").str.strip()

            # Conversion des types avec gestion d'erreurs
            df = df.astype(
                {
                    "code_region": str,
                    "classe": str,
                    "unite_compte": str,
                    "annee": "Int64",
                    "faits": "float64",
                    "population": "float64",
                    "logements": "float64",
                    "taux_pour_mille": "float64",
                }
            )

            # Gestion des valeurs manquantes
            df = df.dropna(subset=["code_region", "classe", "annee", "faits"])

            return df

        except Exception as e:
            logger.error(f"Erreur lors du nettoyage des données: {e}")
            raise

    def _convert_numpy_to_python(self, value):
        """Convertit les types numpy en types Python natifs"""
        if isinstance(value, (np.int64, np.int32)):
            return int(value)
        elif isinstance(value, (np.float64, np.float32)):
            return float(value)
        return value

    def _insert_data_to_mysql(self, table_name: str, df: pd.DataFrame) -> None:
        """Insère les données dans MySQL en utilisant des requêtes optimisées"""
        try:
            conn = mysql.connector.connect(
                host=self.config.HOST,
                user=self.config.USER,
                password=self.config.PASSWORD,
                database=self.config.DATABASE,
            )
            cursor = conn.cursor()

            # Préparation des requêtes selon la table
            if table_name == "regions":
                query = "INSERT IGNORE INTO regions (code_region, nom_region) VALUES (%s, %s)"
                values = list(zip(df["code_region"], df["nom_region"]))
            elif table_name == "categories":
                query = "INSERT IGNORE INTO categories (classe, unite_compte) VALUES (%s, %s)"
                values = list(zip(df["classe"], df["unite_compte"]))
            elif table_name == "statistiques":
                query = """
                INSERT INTO statistiques 
                (code_region, classe, annee, unite_compte, faits, population, logements, taux_pour_mille) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = list(
                    zip(
                        df["code_region"],
                        df["classe"],
                        df["annee"],
                        df["unite_compte"],
                        df["faits"],
                        df["population"],
                        df["logements"],
                        df["taux_pour_mille"],
                    )
                )

            # Convertir les types numpy en types Python natifs
            values = [
                tuple(self._convert_numpy_to_python(value) for value in row)
                for row in values
            ]

            # Exécution par lots pour optimiser les performances
            batch_size = 1000
            for i in range(0, len(values), batch_size):
                batch = values[i : i + batch_size]
                cursor.executemany(query, batch)
                conn.commit()

        except Error as e:
            logger.error(f"Erreur lors de l'insertion dans MySQL: {e}")
            raise
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    def load_data(self, source: str = "csv", file_path: Optional[str] = None) -> None:
        """Charge et traite les données depuis un CSV vers MySQL"""
        try:
            if source == "csv" and file_path:
                logger.info(f"Lecture du fichier CSV: {file_path}")

                # Lecture et nettoyage avec Pandas
                df = pd.read_csv(
                    file_path,
                    sep=";",
                    encoding="utf-8",
                    decimal=",",  # Ajoutez ce paramètre pour gérer les séparateurs décimaux
                )
                df = self._clean_data(df)
                logger.info(f"Données nettoyées: {len(df)} lignes")

                # Préparation des DataFrames pour chaque table
                regions_df = df[["code_region"]].drop_duplicates()
                regions_df["nom_region"] = "Region " + regions_df["code_region"]

                categories_df = df[["classe", "unite_compte"]].drop_duplicates()

                # Insertion dans MySQL table par table
                logger.info("Insertion des données dans MySQL...")
                self._insert_data_to_mysql("regions", regions_df)
                self._insert_data_to_mysql("categories", categories_df)
                self._insert_data_to_mysql("statistiques", df)

                logger.info("Chargement des données terminé avec succès")

            else:
                raise ValueError("Source non valide ou fichier manquant")

        except Exception as e:
            logger.error(f"Erreur lors du chargement des données: {e}")
            raise
