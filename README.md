# criminal-data-scrapper

Projet final pour le cours de génie logiciel de Sorbonne Université

## Description du projet

Ce projet est une application de scraping de données criminelles, d'analyse et de visualisation. Il permet de collecter des données sur les crimes, de les stocker dans une base de données, de les analyser et de les visualiser à l'aide d'une interface utilisateur interactive.

## Prérequis

- Python 3.8 ou supérieur
- MySQL 8.0 ou supérieur

## Installation

1. Clonez le dépôt :
   ```bash
   git clone https://github.com/CocoLng/criminal-data-scrapper.git
   cd criminal-data-scrapper
   ```

2. Créez un environnement virtuel et activez-le :
   ```bash
   python -m venv venv
   source venv/bin/activate  # Sur Windows, utilisez `venv\Scripts\activate`
   ```

3. Installez les dépendances :
   ```bash
   pip install -r requirements.txt
   ```

4. Configurez la base de données :
   - Créez un fichier `.env` à la racine du projet avec les informations de connexion MySQL :
     ```
     MYSQL_HOST=localhost
     MYSQL_USER=root
     MYSQL_PASSWORD=your_password
     MYSQL_DATABASE=delinquance_db
     ```

5. Initialisez la base de données :
   ```bash
   python main.py
   ```

## Structure du projet

- `app.py` : Contient la logique de l'interface utilisateur et les interactions avec les services.
- `database/` : Contient les modules liés à la base de données.
  - `data_loader.py` : Charge et nettoie les données depuis un fichier CSV vers MySQL.
  - `database.py` : Gère les connexions et les requêtes à la base de données.
  - `db_config.py` : Contient la configuration de la base de données.
  - `init_db.py` : Initialise la base de données et charge les données initiales.
- `utils/` : Contient les services d'analyse et de prédiction.
  - `predictive_service.py` : Gère les analyses prédictives.
  - `queries.py` : Contient les requêtes SQL prédéfinies.
  - `security_service.py` : Gère les analyses de sécurité.
  - `territorial_service.py` : Gère les analyses territoriales.
- `view/` : Contient les modules de visualisation.
  - `predictive_view.py` : Gère les visualisations pour les analyses prédictives.
  - `security_view.py` : Gère les visualisations pour les analyses de sécurité.
  - `territorial_view.py` : Gère les visualisations pour les analyses territoriales.
- `main.py` : Point d'entrée principal de l'application.

## Utilisation

1. Lancez l'application :
   ```bash
   python main.py
   ```

2. Accédez à l'interface utilisateur via votre navigateur à l'adresse `http://localhost:7860`.

## Contribution

Les contributions sont les bienvenues ! Pour contribuer :

1. Forkez le dépôt.
2. Créez une branche pour votre fonctionnalité (`git checkout -b feature/ma-fonctionnalite`).
3. Commitez vos modifications (`git commit -am 'Ajout de ma fonctionnalité'`).
4. Poussez votre branche (`git push origin feature/ma-fonctionnalite`).
5. Ouvrez une Pull Request.

## Contact

Pour toute question ou suggestion, veuillez crée une issue sur le repo
