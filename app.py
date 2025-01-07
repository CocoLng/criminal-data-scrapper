import logging
from typing import Tuple

import gradio as gr
import pandas as pd

from database.database import DatabaseConnection
from utils.predictive_service import PredictiveService
from utils.queries import QueryBuilder
from utils.security_service import SecurityService
from utils.territorial_service import TerritorialService

logger = logging.getLogger(__name__)


class InterfaceManager:
    def __init__(self):
        self.db = DatabaseConnection()
        self.query_builder = QueryBuilder()
        self.security_service = SecurityService()
        self.territorial_service = TerritorialService()
        self.predictive_service = PredictiveService()
        self.regions = sorted(
            self.db.get_distinct_values("departements", "code_region")
        )
        self._load_initial_values()

    def _load_initial_values(self):
        """Load all necessary values from database"""
        try:
            self.types_crimes = self.db.get_distinct_values("crimes", "type_crime")
            self.annees = sorted(self.db.get_distinct_values("crimes", "annee"))
            self.departements = sorted(
                self.db.get_distinct_values("departements", "code_departement")
            )
            self.regions = sorted(
                self.db.get_distinct_values("departements", "code_region")
            )
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
        code_departement: str = None,
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

    def execute_service(
        self, category: str, service: str, **params
    ) -> Tuple[pd.DataFrame, str]:
        """Execute a service analysis"""
        return self.service_manager.execute_service(category, service, params)


def create_and_launch_interface(share=False, server_name="0.0.0.0", server_port=7860):
    interface_manager = InterfaceManager()

    def initialize_plot_visibility(security_service: str = "Sécurité Immobilière"):
        """Initialize plot visibility based on security service"""
        if security_service == "Sécurité Immobilière":
            return [
                gr.update(visible=True, label="Évolution de la criminalité"),
                gr.update(visible=True, label="Analyse comparative"),
                gr.update(visible=True, label="Indicateurs de risque"),
                gr.update(visible=True, label="Tendances saisonnières"),
            ]
        else:
            return [
                gr.update(visible=True, label="Analyse principale"),
                gr.update(visible=True, label="Données complémentaires"),
                gr.update(visible=False),
                gr.update(visible=False),
            ]

    with gr.Blocks(title="Analyse de la Délinquance") as interface:
        current_tab = gr.State("security")
        current_service = gr.State("Sécurité Immobilière")
        gr.Markdown("# 🚨 Interface d'analyse de la délinquance")

        with gr.Tabs() as tabs:
            # Onglet Sécurité
            with gr.Tab("Sécurité"):
                with gr.Row():
                    security_service = gr.Dropdown(
                        choices=[
                            "Sécurité Immobilière",
                            "AlerteVoisinage",
                            "BusinessSecurity",
                            "OptimisationAssurance",
                            "TransportSécurité",
                        ],
                        label="Service de sécurité",
                        value="Sécurité Immobilière"
                    )

                with gr.Row():
                    dept_security = gr.Dropdown(
                        choices=interface_manager.departements,
                        label="Département",
                        value="75",
                        visible=True,
                    )
                    dept_security_dest = gr.Dropdown(
                        choices=interface_manager.departements,
                        label="Département de destination",
                        value="13",
                        visible=False,
                    )
                    year_security = gr.Dropdown(
                        choices=interface_manager.annees,
                        label="Année",
                        value=21,
                        visible=True,
                    )

                with gr.Row():
                    crime_type_security = gr.Dropdown(
                        choices=interface_manager.types_crimes,
                        label="Type de crime",
                        visible=False,
                    )

                security_button = gr.Button("Analyser")

            # Onglet Analyse Territoriale
            with gr.Tab("Analyse Territoriale"):
                with gr.Row():
                    territorial_service = gr.Dropdown(
                        choices=[
                            "Diagnostic Régional",
                            "Comparaison Inter-Régionale",
                            "Évolution Régionale",
                        ],
                        label="Service d'analyse territoriale",
                    )

                with gr.Row():
                    region_reference = gr.Dropdown(
                        choices=interface_manager.regions,
                        label="Région de référence",
                        value="75",
                        visible=True,
                    )
                    region_comparison = gr.Dropdown(
                        choices=interface_manager.regions,
                        label="Région à comparer",
                        visible=False,
                    )

                territorial_button = gr.Button("Analyser")

            # Onglet Prédiction
            with gr.Tab("Prédiction"):
                with gr.Row():
                    prediction_service = gr.Dropdown(
                        choices=[
                            "Projection Criminelle",
                            "Analyse des Risques Émergents",
                        ],
                        label="Service de prédiction",
                        value="Projection Criminelle",
                    )

                with gr.Row():
                    dept_pred = gr.Dropdown(
                        choices=interface_manager.departements,
                        label="Département",
                        value=interface_manager.departements[0] if interface_manager.departements else None,
                    )
                    crime_type_pred = gr.Dropdown(
                        choices=interface_manager.types_crimes,
                        label="Type de crime",
                        value=interface_manager.types_crimes[0] if interface_manager.types_crimes else None,
                        visible=True,
                    )
                    target_year = gr.Number(
                        label="Année finale de prédiction (ex: 26 pour 2026)",
                        value=25,
                        minimum=24,
                        maximum=30,
                        step=1,
                        precision=0,
                        visible=True,
                    )

                prediction_button = gr.Button("Analyser")

            # Onglet Requêtes simples
            with gr.Tab("Requêtes simples"):
                with gr.Row():
                    query_dropdown = gr.Dropdown(
                        choices=list(QueryBuilder.get_predefined_queries().keys()),
                        label="Type d'analyse",
                    )

                with gr.Row():
                    type_crime = gr.Dropdown(
                        choices=interface_manager.types_crimes,
                        label="Type de crime",
                        visible=True,
                    )
                    annee = gr.Dropdown(
                        choices=interface_manager.annees, 
                        label="Année", 
                        visible=True
                    )
                    code_departement = gr.Dropdown(
                        choices=interface_manager.departements,
                        label="Département",
                        visible=True,
                    )

                query_button = gr.Button("Exécuter")

            # Onglet Requêtes avancées
            with gr.Tab("Requêtes avancées"):
                custom_query = gr.Textbox(
                    lines=5,
                    placeholder="Entrez votre requête SQL ici...",
                    label="Requête SQL personnalisée",
                )
                advanced_button = gr.Button("Exécuter")

            # Zone de résultats commune
            with gr.Column():
                gr.Markdown("## Visualisations")

                with gr.Row():
                    plot1 = gr.Plot(label="Évolution de la criminalité", visible=True)
                    plot2 = gr.Plot(label="Analyse comparative", visible=True)
                with gr.Row():
                    plot3 = gr.Plot(label="Indicateurs de risque", visible=True)
                    plot4 = gr.Plot(label="Tendances saisonnières", visible=True)

                gr.Markdown("## Analyse et Recommandations")
                insights = gr.Textbox(lines=3)

                gr.Markdown("## Résultats détaillés")
                output_df = gr.DataFrame(label="Résultats", interactive=False)

                # Fonctions de mise à jour des plots
                def hide_extra_plots():
                    current_tab.value = "autre"
                    return initialize_plot_visibility("autre")

                def update_security_plots(service):
                    current_service.value = service
                    return initialize_plot_visibility(service)

                def on_tab_change(tab_name):
                    if tab_name == "Sécurité":
                        current_tab.value = "security"
                        return initialize_plot_visibility(current_service.value)
                    else:
                        current_tab.value = "autre"
                        return initialize_plot_visibility("autre")

                # Event handlers
                security_service.change(
                    fn=update_security_plots,
                    inputs=[security_service],
                    outputs=[plot1, plot2, plot3, plot4],
                )

                # Event handlers pour les changements d'onglets
                for tab_name in ["Analyse Territoriale", "Prédiction", "Requêtes simples", "Requêtes avancées"]:
                    tabs.select(
                        fn=on_tab_change,
                        inputs=tabs,
                        outputs=[plot1, plot2, plot3, plot4],
                    ).then(
                        fn=hide_extra_plots,
                        inputs=None,
                        outputs=[plot1, plot2, plot3, plot4],
                        api_name="hide_plots_other_tabs"
                    )

                # Event handlers pour les boutons
                security_button.click(
                    fn=interface_manager.security_service.process_request,
                    inputs=[
                        security_service,
                        dept_security,
                        year_security,
                        dept_security_dest,
                        crime_type_security,
                    ],
                    outputs=[output_df, insights, plot1, plot2, plot3, plot4],
                )

                territorial_button.click(
                    fn=interface_manager.territorial_service.process_request,
                    inputs=[territorial_service, region_reference, region_comparison],
                    outputs=[output_df, insights, plot1, plot2],
                )

                prediction_button.click(
                    fn=interface_manager.predictive_service.process_request,
                    inputs=[prediction_service, dept_pred, crime_type_pred, target_year],
                    outputs=[output_df, insights, plot1, plot2],
                )

                query_button.click(
                    fn=interface_manager.execute_predefined_query,
                    inputs=[query_dropdown, type_crime, annee, code_departement],
                    outputs=[output_df, insights],
                )

                advanced_button.click(
                    fn=interface_manager.execute_custom_query,
                    inputs=[custom_query],
                    outputs=[output_df, insights],
                )

                # Handlers pour la visibilité des champs
                def update_security_fields(service):
                    if service == "TransportSécurité":
                        return {
                            dept_security: gr.update(
                                visible=True, label="Département de départ"
                            ),
                            dept_security_dest: gr.update(visible=True),
                            year_security: gr.update(visible=False),
                            crime_type_security: gr.update(visible=False),
                        }
                    elif service == "AlerteVoisinage":
                        return {
                            dept_security: gr.update(visible=True, label="Département"),
                            dept_security_dest: gr.update(visible=False),
                            year_security: gr.update(visible=True),
                            crime_type_security: gr.update(visible=False),
                        }
                    elif service == "BusinessSecurity":
                        return {
                            dept_security: gr.update(visible=True, label="Département"),
                            dept_security_dest: gr.update(visible=False),
                            year_security: gr.update(visible=False),
                            crime_type_security: gr.update(visible=False),
                        }
                    else:
                        return {
                            dept_security: gr.update(visible=True, label="Département"),
                            dept_security_dest: gr.update(visible=False),
                            year_security: gr.update(visible=True),
                            crime_type_security: gr.update(visible=False),
                        }

                def update_territorial_fields(service):
                    if service == "Comparaison Inter-Régionale":
                        return {
                            region_reference: gr.update(
                                visible=True, label="Région de référence"
                            ),
                            region_comparison: gr.update(visible=True),
                        }
                    else:
                        return {
                            region_reference: gr.update(visible=True, label="Région"),
                            region_comparison: gr.update(visible=False),
                        }

                security_service.change(
                    fn=update_security_plots,
                    inputs=[security_service],
                    outputs=[plot1, plot2, plot3, plot4],
                )

                territorial_service.change(
                    fn=update_territorial_fields,
                    inputs=[territorial_service],
                    outputs=[region_reference, region_comparison],
                )

    return interface.launch(
        share=share, server_name=server_name, server_port=server_port
    )