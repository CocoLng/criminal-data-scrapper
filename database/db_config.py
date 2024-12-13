import os
from dataclasses import dataclass, field
from typing import Dict

from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv()


def get_default_tables() -> Dict[str, str]:
    """Returns the default tables configuration"""
    return {
        "regions": """
            CREATE TABLE IF NOT EXISTS regions (
                code_region VARCHAR(10) PRIMARY KEY,
                nom_region VARCHAR(100) NOT NULL,
                INDEX idx_nom_region (nom_region)
            ) ENGINE=InnoDB
        """,
        "categories": """
            CREATE TABLE IF NOT EXISTS categories (
                id_categorie INT AUTO_INCREMENT PRIMARY KEY,
                classe VARCHAR(100) NOT NULL,
                unite_compte VARCHAR(50) NOT NULL,
                UNIQUE KEY unique_classe (classe),
                INDEX idx_unite_compte (unite_compte)
            ) ENGINE=InnoDB
        """,
        "statistiques": """
            CREATE TABLE IF NOT EXISTS statistiques (
                id INT AUTO_INCREMENT PRIMARY KEY,
                code_region VARCHAR(10),
                classe VARCHAR(100),
                annee INT NOT NULL,
                unite_compte VARCHAR(50),
                faits INT NOT NULL,
                population INT NOT NULL,
                logements FLOAT,
                taux_pour_mille FLOAT,
                FOREIGN KEY (code_region) REFERENCES regions(code_region),
                FOREIGN KEY (classe) REFERENCES categories(classe),
                INDEX idx_annee (annee),
                INDEX idx_region_annee (code_region, annee),
                INDEX idx_classe_annee (classe, annee)
            ) ENGINE=InnoDB
        """,
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
