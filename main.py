import logging
import os
import sys

from app import create_and_launch_interface

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("main")

def main():
    try:
        logger.info("Démarrage de l'application")
        create_and_launch_interface()
    except KeyboardInterrupt:
        logger.info("Processus interrompu par l'utilisateur.")
    except Exception as e:
        logger.error(f"Une erreur est survenue: {e}")

if __name__ == "__main__":
    main()