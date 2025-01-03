import logging
import gradio as gr
import pandas as pd
from typing import Tuple, Any

from database.database import DatabaseConnection
from utils.queries import QueryBuilder

logger = logging.getLogger(__name__)

def execute_predefined_query(query_name: str, *params) -> Tuple[pd.DataFrame, str]:
    """Execute a predefined query with parameters"""
    try:
        db = DatabaseConnection()
        queries = QueryBuilder.get_predefined_queries()
        query = queries.get(query_name)
        
        if query:
            df = db.execute_query(query, params)
            return df, "Requête exécutée avec succès"
    except Exception as e:
        logger.error(f"Error executing predefined query: {e}")
        return pd.DataFrame(), f"Erreur: {str(e)}"

def execute_custom_query(query: str) -> Tuple[pd.DataFrame, str]:
    """Execute a custom SQL query"""
    try:
        if not QueryBuilder.validate_query(query):
            return pd.DataFrame(), "Requête non autorisée"
        
        db = DatabaseConnection()
        df = db.execute_query(query)
        return df, "Requête exécutée avec succès"
    except Exception as e:
        logger.error(f"Error executing custom query: {e}")
        return pd.DataFrame(), f"Erreur: {str(e)}"

def init_values() -> Tuple[list, list, list]:
    """Initialize dropdown values from database"""
    db = DatabaseConnection()
    try:
        types_crimes = db.get_distinct_values("crimes", "type_crime")
        annees = db.get_distinct_values("crimes", "annee")
        return types_crimes, annees
    except Exception as e:
        logger.error(f"Error initializing values: {e}")
        return [], []

def create_and_launch_interface(share=False, server_name="0.0.0.0", server_port=7860):
    """Create and launch the Gradio interface"""
    
    # Initialize values for dropdowns
    types_crimes, annees = init_values()
    
    # Create interface
    with gr.Blocks(title="Analyse de la Délinquance") as interface:
        gr.Markdown("# Interface d'analyse de la délinquance")
        
        with gr.Tabs():
            # Simple Query Tab
            with gr.Tab("Requêtes simples"):
                with gr.Row():
                    query_dropdown = gr.Dropdown(
                        choices=list(QueryBuilder.get_predefined_queries().keys()),
                        label="Type de requête"
                    )
                    type_crime = gr.Dropdown(
                        choices=types_crimes,
                        label="Type de crime"
                    )
                    annee = gr.Dropdown(
                        choices=annees,
                        label="Année"
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
            execute_predefined_query,
            inputs=[query_dropdown, type_crime, annee],
            outputs=[output_df, message]
        )
        
        advanced_button.click(
            execute_custom_query,
            inputs=[custom_query],
            outputs=[output_df, message]
        )
    
    # Launch the interface
    interface.launch(
        share=share,
        server_name=server_name,
        server_port=server_port
    )