import logging
from typing import Optional, Tuple

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
        self.engine = create_engine(
            f"mysql+mysqlconnector://{self.config.USER}:{self.config.PASSWORD}@{self.config.HOST}/{self.config.DATABASE}"
        )

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Nettoie et prépare les données avec Pandas"""
        try:
            # Mapping des colonnes
            column_mapping = {
                "Code.département": "code_departement",
                "Code.région": "code_region",
                "classe": "type_crime",
                "annee": "annee",
                "unité.de.compte": "unite_compte",
                "faits": "nombre_faits",
                "POP": "population",
                "LOG": "logements",
                "tauxpourmille": "taux_pour_mille",
            }

            # Renommage des colonnes
            df = df.rename(columns=column_mapping)

            # Nettoyage des strings
            string_columns = [
                "code_departement",
                "code_region",
                "type_crime",
                "unite_compte",
            ]
            df[string_columns] = (
                df[string_columns].astype(str).apply(lambda x: x.str.strip())
            )

            # Remplacer les virgules par des points dans les colonnes numériques
            numeric_columns = [
                "nombre_faits",
                "population",
                "logements",
                "taux_pour_mille",
            ]
            for col in numeric_columns:
                df[col] = df[col].astype(str).str.replace(",", ".").str.strip()

            # Conversion préliminaire en float pour les colonnes numériques
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            # Arrondir les logements à l'entier le plus proche
            df["logements"] = df["logements"].round().astype("Int64")

            # Conversion finale des types
            df = df.astype(
                {
                    "code_departement": str,
                    "code_region": str,
                    "type_crime": str,
                    "unite_compte": str,
                    "annee": "Int64",
                    "nombre_faits": "int64",
                    "population": "int64",
                    "taux_pour_mille": "float64",
                }
            )

            # Gestion des valeurs manquantes
            df = df.dropna(
                subset=["code_departement", "type_crime", "annee", "nombre_faits"]
            )

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

    def _prepare_dataframes(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Prépare les DataFrames pour chaque table"""
        # Préparation crimes
        crimes_df = df[
            ["type_crime", "unite_compte", "annee", "nombre_faits"]
        ].drop_duplicates()

        # Préparation départements (prendre la dernière valeur pour population et logements)
        departements_df = (
            df.groupby("code_departement")
            .agg({"code_region": "first", "population": "last", "logements": "last"})
            .reset_index()
        )

        # Préparation statistiques (nécessitera les id_crime après insertion)
        stats_df = df[["type_crime", "annee", "code_departement", "taux_pour_mille"]]

        return crimes_df, departements_df, stats_df

    def _insert_data_to_mysql(self, table_name: str, df: pd.DataFrame, cursor) -> None:
        """Insère les données dans MySQL en utilisant des requêtes optimisées"""
        try:
            if table_name == "crimes":
                query = """
                INSERT IGNORE INTO crimes 
                (type_crime, unite_compte, annee, nombre_faits) 
                VALUES (%s, %s, %s, %s)
                """
                values = list(
                    zip(
                        df["type_crime"],
                        df["unite_compte"],
                        df["annee"],
                        df["nombre_faits"],
                    )
                )

            elif table_name == "departements":
                query = """
                INSERT IGNORE INTO departements 
                (code_departement, code_region, population, logements) 
                VALUES (%s, %s, %s, %s)
                """
                values = list(
                    zip(
                        df["code_departement"],
                        df["code_region"],
                        df["population"],
                        df["logements"],
                    )
                )

            elif table_name == "statistiques":
                query = """
                INSERT INTO statistiques 
                (id_crime, code_departement, taux_pour_mille)
                SELECT c.id_crime, %s, %s
                FROM crimes c
                WHERE c.type_crime = %s AND c.annee = %s
                """
                values = list(
                    zip(
                        df["code_departement"],
                        df["taux_pour_mille"],
                        df["type_crime"],
                        df["annee"],
                    )
                )

            # Convertir les types numpy
            values = [
                tuple(self._convert_numpy_to_python(value) for value in row)
                for row in values
            ]

            # Insertion par lots
            batch_size = self.config.get_batch_size()
            for i in range(0, len(values), batch_size):
                batch = values[i : i + batch_size]
                cursor.executemany(query, batch)

        except Error as e:
            logger.error(f"Erreur lors de l'insertion dans {table_name}: {e}")
            raise

    def load_data(self, source: str = "csv", file_path: Optional[str] = None) -> None:
        """Charge et traite les données depuis un CSV vers MySQL"""
        try:
            if source == "csv" and file_path:
                logger.info(f"Lecture du fichier CSV: {file_path}")

                # Lecture et nettoyage
                df = pd.read_csv(file_path, sep=";", encoding="utf-8", decimal=",")
                df = self._clean_data(df)
                logger.info(f"Données nettoyées: {len(df)} lignes")

                # Préparation des DataFrames
                crimes_df, departements_df, stats_df = self._prepare_dataframes(df)

                # Connexion à la base de données
                conn = mysql.connector.connect(**self.config.get_connection_params())
                cursor = conn.cursor()

                try:
                    # Insertion des données table par table
                    logger.info("Insertion des crimes...")
                    self._insert_data_to_mysql("crimes", crimes_df, cursor)

                    logger.info("Insertion des départements...")
                    self._insert_data_to_mysql("departements", departements_df, cursor)

                    logger.info("Insertion des statistiques...")
                    self._insert_data_to_mysql("statistiques", stats_df, cursor)

                    conn.commit()
                    logger.info("Chargement des données terminé avec succès")

                finally:
                    cursor.close()
                    conn.close()

            else:
                raise ValueError("Source non valide ou fichier manquant")

        except Exception as e:
            logger.error(f"Erreur lors du chargement des données: {e}")
            raise
