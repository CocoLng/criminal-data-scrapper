import logging
import gradio as gr
import pandas as pd
from typing import Tuple, Any, Dict, List

from database.database import DatabaseConnection
from utils.queries import QueryBuilder

logger = logging.getLogger(__name__)

class InterfaceManager:
    def __init__(self):
        self.db = DatabaseConnection()
        self.query_builder = QueryBuilder()
        self._load_initial_values()

    def _load_initial_values(self):
        """Load all necessary values from database"""
        try:
            self.types_crimes = self.db.get_distinct_values("crimes", "type_crime")
            self.annees = sorted(self.db.get_distinct_values("crimes", "annee"))
            self.departements = sorted(self.db.get_distinct_values("departements", "code_departement"))
            self.regions = sorted(self.db.get_distinct_values("departements", "code_region"))
        except Exception as e:
            logger.error(f"Error loading initial values: {e}")
            self.types_crimes = []
            self.annees = []
            self.departements = []
            self.regions = []

    def execute_predefined_query(
        self, 
        query_name: str,
        type_crime: str = None,
        annee: int = None,
        code_departement: str = None
    ) -> Tuple[pd.DataFrame, str]:
        """Execute a predefined query with parameters"""
        try:
            queries = self.query_builder.get_predefined_queries()
            query_info = queries.get(query_name)
            
            if not query_info:
                return pd.DataFrame(), "Requête non trouvée"

            # Prepare parameters based on query requirements
            params = []
            for param_name in query_info["params"]:
                if param_name == "type_crime" and type_crime:
                    params.append(type_crime)
                elif param_name == "annee" and annee:
                    params.append(annee)
                elif param_name == "code_departement" and code_departement:
                    params.append(code_departement)

            if len(params) != len(query_info["params"]):
                return pd.DataFrame(), "Paramètres manquants"

            df = self.db.execute_query(query_info["query"], tuple(params))
            return df, "Requête exécutée avec succès"
            
        except Exception as e:
            logger.error(f"Error executing predefined query: {e}")
            return pd.DataFrame(), f"Erreur: {str(e)}"

    def execute_custom_query(self, query: str) -> Tuple[pd.DataFrame, str]:
        """Execute a custom SQL query"""
        try:
            if not self.query_builder.validate_query(query):
                return pd.DataFrame(), "Requête non autorisée"
            
            df = self.db.execute_query(query)
            return df, "Requête exécutée avec succès"
        except Exception as e:
            logger.error(f"Error executing custom query: {e}")
            return pd.DataFrame(), f"Erreur: {str(e)}"

def create_and_launch_interface(share=False, server_name="0.0.0.0", server_port=7860):
    """Create and launch the Gradio interface"""
    
    interface_manager = InterfaceManager()
    
    # Create interface
    with gr.Blocks(title="Analyse de la Délinquance") as interface:
        gr.Markdown("# Interface d'analyse de la délinquance")
        
        with gr.Tabs():
            # Simple Query Tab
            with gr.Tab("Requêtes simples"):
                with gr.Row():
                    query_dropdown = gr.Dropdown(
                        choices=list(QueryBuilder.get_predefined_queries().keys()),
                        label="Type d'analyse",
                        info="Sélectionnez le type d'analyse souhaité"
                    )
                
                with gr.Row():
                    type_crime = gr.Dropdown(
                        choices=interface_manager.types_crimes,
                        label="Type de crime",
                        visible=True
                    )
                    annee = gr.Dropdown(
                        choices=interface_manager.annees,
                        label="Année",
                        visible=True
                    )
                    code_departement = gr.Dropdown(
                        choices=interface_manager.departements,
                        label="Département",
                        visible=True
                    )
                
                def update_param_visibility(query_name):
                    """Update parameter visibility based on selected query"""
                    if not query_name:
                        return {
                            type_crime: gr.update(visible=False),
                            annee: gr.update(visible=False),
                            code_departement: gr.update(visible=False)
                        }
                    
                    query_info = QueryBuilder.get_predefined_queries()[query_name]
                    params = query_info["params"]
                    
                    return {
                        type_crime: gr.update(visible="type_crime" in params),
                        annee: gr.update(visible="annee" in params),
                        code_departement: gr.update(visible="code_departement" in params)
                    }
                
                query_dropdown.change(
                    update_param_visibility,
                    inputs=[query_dropdown],
                    outputs=[type_crime, annee, code_departement]
                )
                
                query_button = gr.Button("Exécuter")
                
            # Advanced Query Tab
            with gr.Tab("Requêtes avancées"):
                custom_query = gr.Textbox(
                    lines=5,
                    placeholder="Entrez votre requête SQL ici...",
                    label="Requête SQL personnalisée"
                )
                advanced_button = gr.Button("Exécuter")
            
            # Results area (shared between tabs)
            output_df = gr.DataFrame(label="Résultats")
            message = gr.Textbox(label="Messages")
        
        # Set up event handlers
        query_button.click(
            interface_manager.execute_predefined_query,
            inputs=[query_dropdown, type_crime, annee, code_departement],
            outputs=[output_df, message]
        )
        
        advanced_button.click(
            interface_manager.execute_custom_query,
            inputs=[custom_query],
            outputs=[output_df, message]
        )
    
    # Launch the interface
    interface.launch(
        share=share,
        server_name=server_name,
        server_port=server_port
    )