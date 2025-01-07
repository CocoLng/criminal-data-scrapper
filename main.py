import logging
import traceback

from app import create_and_launch_interface
from database.init_db import DatabaseInitializer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log", mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("main")


def main():
    try:
        logger.info("Démarrage de l'application")
        # Initialisation de la base de données
        db_init = DatabaseInitializer()
        db_init.create_database()

        # Paramètres pour le lancement de l'interface
        share = False
        server_name = "0.0.0.0"
        server_port = 7860

        # Création et lancement de l'interface
        create_and_launch_interface(
            share=share, server_name=server_name, server_port=server_port
        )
    except KeyboardInterrupt:
        logger.info("Processus interrompu par l'utilisateur.")
    except Exception as e:
        logger.error(f"Une erreur est survenue: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
