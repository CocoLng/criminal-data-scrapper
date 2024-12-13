import logging
import os
import sys
import traceback

from database.init_db import DatabaseInitializer
from app import create_and_launch_interface


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log', mode='w'),  # Assurez-vous que le mode est 'w'
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("main")

def main():
    try:
        logger.info("Démarrage de l'application")
        # Initialize database and load data
        db_init = DatabaseInitializer()
        db_init.create_database()
        
        # Paramètres pour le lancement de l'interface
        share = False
        server_name = "0.0.0.0"
        server_port = 7860
        
        # Start the application
        create_and_launch_interface(share=share, server_name=server_name, server_port=server_port)
    except KeyboardInterrupt:
        logger.info("Processus interrompu par l'utilisateur.")
    except Exception as e:
        logger.error(f"Une erreur est survenue: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()