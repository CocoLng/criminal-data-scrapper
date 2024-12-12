from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    HOST: str = 'localhost'
    USER: str = 'root'
    PASSWORD: str = 'your_password'  # À modifier selon votre configuration
    DATABASE: str = 'delinquance_db'
    
    # Définition des tables
    TABLES = {
        'regions': """
            CREATE TABLE IF NOT EXISTS regions (
                code_region VARCHAR(10) PRIMARY KEY,
                nom_region VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB
        """,
        
        'categories': """
            CREATE TABLE IF NOT EXISTS categories (
                id_categorie INT AUTO_INCREMENT PRIMARY KEY,
                classe VARCHAR(100) NOT NULL,
                unite_compte VARCHAR(50) NOT NULL,
                UNIQUE KEY unique_classe (classe)
            ) ENGINE=InnoDB
        """,
        
        'statistiques': """
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
                FOREIGN KEY (classe) REFERENCES categories(classe)
            ) ENGINE=InnoDB
        """
    }
    
    @classmethod
    def get_connection_params(cls):
        return {
            'host': cls.HOST,
            'user': cls.USER,
            'password': cls.PASSWORD,
            'database': cls.DATABASE
        }