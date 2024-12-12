import gradio as gr
import pandas as pd
import plotly.express as px
import mysql.connector
import logging
from database.db_config import DatabaseConfig

logger = logging.getLogger(__name__)

class DelinquanceApp:
    def __init__(self):
        self.db_config = DatabaseConfig()
        self.conn = self._create_db_connection()

    def _create_db_connection(self):
        """Crée une connexion à la base de données"""
        try:
            return mysql.connector.connect(
                host=self.db_config.HOST,
                user=self.db_config.USER,
                password=self.db_config.PASSWORD,
                database=self.db_config.DATABASE
            )
        except Exception as e:
            logger.error(f"Erreur de connexion à la BD: {e}")
            raise

    def get_statistics(self, region=None, annee=None, type_delit=None):
        """Récupère les statistiques selon les filtres"""
        try:
            cursor = self.conn.cursor(dictionary=True)
            
            query = """
                SELECT r.nom_region, s.annee, s.faits, s.taux_pour_mille, c.classe, c.unite_compte
                FROM statistiques s
                JOIN regions r ON s.code_region = r.code_region
                JOIN categories c ON s.id_categorie = c.id_categorie
                WHERE 1=1
            """
            params = []

            if region:
                query += " AND r.nom_region = %s"
                params.append(region)
            if annee:
                query += " AND s.annee = %s"
                params.append(annee)
            if type_delit:
                query += " AND c.classe = %s"
                params.append(type_delit)

            cursor.execute(query, params)
            return pd.DataFrame(cursor.fetchall())
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des statistiques: {e}")
            raise
        finally:
            cursor.close()

    def create_map(self, annee, type_delit=None):
        """Crée une carte choroplèthe de la France"""
        df = self.get_statistics(annee=annee, type_delit=type_delit)
        fig = px.choropleth(
            df,
            locations='nom_region',
            locationmode='country names',
            color='taux_pour_mille',
            hover_data=['faits'],
            title=f'Taux de délinquance en {annee}'
        )
        return fig

    def create_time_series(self, region=None, type_delit=None):
        """Crée un graphique d'évolution temporelle"""
        df = self.get_statistics(region=region, type_delit=type_delit)
        fig = px.line(
            df.groupby('annee')['faits'].sum().reset_index(),
            x='annee',
            y='faits',
            title='Évolution temporelle des faits'
        )
        return fig

def create_and_launch_interface():
    """Crée et lance l'interface Gradio"""
    app = DelinquanceApp()
    logger.info("Création de l'interface Gradio")

    with gr.Blocks() as interface:
        gr.Markdown("# Analyse de la Délinquance en France")
        
        with gr.Row():
            with gr.Column():
                # Filtres
                annee = gr.Dropdown(
                    choices=[str(i) for i in range(2016, 2024)],
                    value="2023",
                    label="Année"
                )
                region = gr.Dropdown(
                    choices=["Toutes"] + list(app.get_statistics()['nom_region'].unique()),
                    value="Toutes",
                    label="Région"
                )
                type_delit = gr.Dropdown(
                    choices=["Tous"] + list(app.get_statistics()['classe'].unique()),
                    value="Tous",
                    label="Type de délit"
                )

        with gr.Tabs():
            with gr.TabItem("Carte"):
                carte_plot = gr.Plot()
                annee.change(
                    fn=app.create_map,
                    inputs=[annee, type_delit],
                    outputs=carte_plot
                )

            with gr.TabItem("Évolution Temporelle"):
                series_plot = gr.Plot()
                region.change(
                    fn=app.create_time_series,
                    inputs=[region, type_delit],
                    outputs=series_plot
                )

    interface.launch(share=False)

if __name__ == "__main__":
    create_and_launch_interface()