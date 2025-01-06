import os
from dataclasses import dataclass, field
from typing import Dict

from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv()

def get_default_tables() -> Dict[str, str]:
    """Retourne la configuration par défaut des tables"""
    return {
        "crimes": """
            CREATE TABLE IF NOT EXISTS crimes (
                id_crime INT AUTO_INCREMENT PRIMARY KEY,
                type_crime VARCHAR(100) NOT NULL,
                unite_compte VARCHAR(50) NOT NULL,
                annee INT NOT NULL,
                nombre_faits INT NOT NULL,
                UNIQUE KEY unique_crime_annee (type_crime, annee),
                INDEX idx_type_crime (type_crime),
                INDEX idx_annee (annee)
            ) ENGINE=InnoDB
        """,
        "departements": """
            CREATE TABLE IF NOT EXISTS departements (
                code_departement VARCHAR(3) PRIMARY KEY,
                code_region VARCHAR(2) NOT NULL,
                population INT NOT NULL,
                logements INT NOT NULL,
                INDEX idx_code_region (code_region)
            ) ENGINE=InnoDB
        """,
        "statistiques": """
            CREATE TABLE IF NOT EXISTS statistiques (
                id_statistique INT AUTO_INCREMENT PRIMARY KEY,
                id_crime INT NOT NULL,
                code_departement VARCHAR(3) NOT NULL,
                taux_pour_mille FLOAT NOT NULL,
                FOREIGN KEY (id_crime) REFERENCES crimes(id_crime),
                FOREIGN KEY (code_departement) REFERENCES departements(code_departement),
                UNIQUE KEY unique_stat (id_crime, code_departement),
                INDEX idx_departement (code_departement),
                INDEX idx_crime_dept (id_crime, code_departement)
            ) ENGINE=InnoDB
        """
    }


@dataclass
class DatabaseConfig:
    HOST: str = os.getenv("MYSQL_HOST", "localhost")
    USER: str = os.getenv("MYSQL_USER", "root")
    PASSWORD: str = os.getenv("MYSQL_PASSWORD", "")
    DATABASE: str = os.getenv("MYSQL_DATABASE", "delinquance_db")
    TABLES: Dict[str, str] = field(default_factory=get_default_tables)

    @classmethod
    def get_connection_params(cls) -> Dict[str, str]:
        """Retourne les paramètres de connexion"""
        return {
            "host": cls.HOST,
            "user": cls.USER,
            "password": cls.PASSWORD,
            "database": cls.DATABASE,
        }

    @classmethod
    def get_batch_size(cls) -> int:
        """Retourne la taille des lots pour les insertions"""
        return 1000  # Taille optimale pour les insertions en lot