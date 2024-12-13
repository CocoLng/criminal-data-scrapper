import gradio as gr
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector
import logging
from database.db_config import DatabaseConfig
from typing import Optional, Dict, Any, Tuple

logger = logging.getLogger(__name__)

class DelinquanceApp:
    def __init__(self):
        self.db_config = DatabaseConfig()
        self.conn = self._create_db_connection()

    def _create_db_connection(self) -> mysql.connector.connection.MySQLConnection:
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

    def _execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Exécute une requête SQL et retourne un DataFrame Pandas"""
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(query, params or ())
            data = cursor.fetchall()
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution de la requête: {e}")
            raise
        finally:
            cursor.close()

    def get_statistics(self, region: Optional[str] = None, 
                      annee: Optional[str] = None, 
                      type_delit: Optional[str] = None) -> pd.DataFrame:
        """Récupère les statistiques selon les filtres"""
        try:
            query = """
                SELECT 
                    r.nom_region, r.code_region,
                    s.annee, s.faits, s.taux_pour_mille,
                    c.classe, c.unite_compte,
                    s.population, s.logements
                FROM statistiques s
                JOIN regions r ON s.code_region = r.code_region
                JOIN categories c ON s.classe = c.classe
                WHERE 1=1
            """
            params = []

            if region and region != "Toutes":
                query += " AND r.code_region = %s"
                params.append(region)
            if annee:
                query += " AND s.annee = %s"
                params.append(int(annee))
            if type_delit and type_delit != "Tous":
                query += " AND c.classe = %s"
                params.append(type_delit)

            # Exécution de la requête et conversion en DataFrame
            df = self._execute_query(query, tuple(params))
            return df

        except Exception as e:
            logger.error(f"Erreur lors de la récupération des statistiques: {e}")
            raise

    def create_map(self, annee: str, type_delit: Optional[str] = None) -> go.Figure:
        """Crée une carte de la France avec les statistiques"""
        df = self.get_statistics(annee=annee, type_delit=type_delit)
        
        # Création de la carte
        fig = px.choropleth(
            df,
            geojson="https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/regions.geojson",
            featureidkey="properties.nom",
            locations="nom_region",
            color="taux_pour_mille",
            color_continuous_scale="Viridis",
            title=f"Carte de la délinquance en France en {annee}",
            labels={"taux_pour_mille": "Taux pour mille"}
        )
        fig.update_geos(fitbounds="locations", visible=False)
        return fig

    def create_time_series(self, region: Optional[str] = None, 
                          type_delit: Optional[str] = None) -> go.Figure:
        """Crée un graphique d'évolution temporelle"""
        df = self.get_statistics(region=region, type_delit=type_delit)
        
        # Utilisation de Pandas pour l'agrégation des données
        grouped_df = df.groupby(['annee', 'classe'])['faits'].sum().reset_index()
        
        fig = px.line(
            grouped_df,
            x='annee',
            y='faits',
            color='classe',
            title='Évolution temporelle des faits'
        )
        return fig

    def get_filter_options(self) -> Tuple[list, list, list]:
        """Récupère les options pour les filtres depuis la base de données"""
        years = self._execute_query("SELECT DISTINCT annee FROM statistiques ORDER BY annee")
        regions = self._execute_query("SELECT DISTINCT code_region, nom_region FROM regions")
        delits = self._execute_query("SELECT DISTINCT classe FROM categories ORDER BY classe")
        
        return (
            years['annee'].tolist(),
            [("Toutes", "Toutes")] + list(zip(regions['code_region'], regions['nom_region'])),
            ["Tous"] + delits['classe'].tolist()
        )

def create_and_launch_interface(share: bool, server_name: str, server_port: int):
    """Crée et lance l'interface Gradio"""
    app = DelinquanceApp()
    years, regions, delits = app.get_filter_options()
    
    logger.info("Création de l'interface Gradio")

    with gr.Blocks() as interface:
        gr.Markdown("# Analyse de la Délinquance en France")
        
        with gr.Row():
            with gr.Column():
                annee = gr.Dropdown(
                    choices=[str(i) for i in years],
                    value=str(max(years)),
                    label="Année"
                )
                region = gr.Dropdown(
                    choices=regions,
                    value="Toutes",
                    label="Région"
                )
                type_delit = gr.Dropdown(
                    choices=delits,
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
                for input_component in [region, type_delit]:
                    input_component.change(
                        fn=app.create_time_series,
                        inputs=[region, type_delit],
                        outputs=series_plot
                    )

    interface.launch(share=share, server_name=server_name, server_port=server_port, debug=True)