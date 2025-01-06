import logging
import gradio as gr
import pandas as pd
from typing import Tuple

from database.database import DatabaseConnection
from utils.queries import QueryBuilder
from utils.security_service import SecurityService
from utils.territorial_service import TerritorialService
from utils.predictive_service import PredictiveService

logger = logging.getLogger(__name__)


class InterfaceManager:
    def __init__(self):
        self.db = DatabaseConnection()
        self.query_builder = QueryBuilder()
        self.security_service = SecurityService()
        self.territorial_service = TerritorialService()
        self.predictive_service = PredictiveService()
        self.regions = sorted(self.db.get_distinct_values("departements", "code_region"))
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

    with gr.Blocks(title="Analyse de la Délinquance") as interface:
        gr.Markdown("# Interface d'analyse de la délinquance")

        with gr.Tabs():
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
                    )

                with gr.Row():
                    dept_security = gr.Dropdown(
                        choices=interface_manager.departements,
                        label="Département",
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

                # Fonction pour gérer la visibilité des champs en fonction du service sélectionné
                def update_security_fields(service):
                    if service == "TransportSécurité":
                        return {
                            dept_security: gr.update(visible=True, label="Département de départ"),
                            dept_security_dest: gr.update(visible=True),
                            year_security: gr.update(visible=False),
                            crime_type_security: gr.update(visible=False)
                        }
                    elif service == "AlerteVoisinage":
                        return {
                            dept_security: gr.update(visible=True, label="Département"),
                            dept_security_dest: gr.update(visible=False),
                            year_security: gr.update(visible=True),
                            crime_type_security: gr.update(visible=False)
                        }
                    elif service == "BusinessSecurity":
                        return {
                            dept_security: gr.update(visible=True, label="Département"),
                            dept_security_dest: gr.update(visible=False),
                            year_security: gr.update(visible=False)
                        }
                    else:
                        return {
                            dept_security: gr.update(visible=True, label="Département"),
                            dept_security_dest: gr.update(visible=False),
                            year_security: gr.update(visible=True),
                            crime_type_security: gr.update(visible=False)
                        }

                # Connexion de la fonction de mise à jour avec le dropdown de service
                security_service.change(
                    fn=update_security_fields,
                    inputs=[security_service],
                    outputs=[
                        dept_security,
                        dept_security_dest,
                        year_security,
                        crime_type_security,
                    ],
                )

            # Onglet Analyse Territoriale
            with gr.Tab("Analyse Territoriale"):
                with gr.Row():
                    territorial_service = gr.Dropdown(
                        choices=[
                            "Diagnostic Régional",
                            "Comparaison Inter-Régionale",
                            "Évolution Régionale"
                        ],
                        label="Service d'analyse territoriale"
                    )

                with gr.Row():
                    region_reference = gr.Dropdown(
                        choices=interface_manager.regions,
                        label="Région de référence",
                        visible=True
                    )
                    region_comparison = gr.Dropdown(
                        choices=interface_manager.regions,
                        label="Région à comparer",
                        visible=False
                    )

                territorial_button = gr.Button("Analyser")

                # Fonction pour gérer la visibilité des champs en fonction du service sélectionné
                def update_territorial_fields(service):
                    if service == "Comparaison Inter-Régionale":
                        return {
                            region_reference: gr.update(visible=True, label="Région de référence"),
                            region_comparison: gr.update(visible=True)
                        }
                    else:
                        return {
                            region_reference: gr.update(visible=True, label="Région"),
                            region_comparison: gr.update(visible=False)
                        }

                # Connexion de la fonction de mise à jour avec le dropdown de service
                territorial_service.change(
                    fn=update_territorial_fields,
                    inputs=[territorial_service],
                    outputs=[region_reference, region_comparison]
                )

            # Onglet Prédiction
            with gr.Tab("Prédiction"):
                with gr.Row():
                    prediction_service = gr.Dropdown(
                        choices=["Prévision Saisonnière", "PolicePrédictive"],
                        label="Service de prédiction",
                    )

                with gr.Row():
                    crime_type_pred = gr.Dropdown(
                        choices=interface_manager.types_crimes, label="Type de crime"
                    )
                    dept_pred = gr.Dropdown(
                        choices=interface_manager.departements, label="Département"
                    )

                with gr.Row():
                    horizon_pred = gr.Dropdown(
                        choices=["1 mois", "3 mois", "6 mois", "1 an"],
                        label="Horizon de prédiction",
                    )

                prediction_button = gr.Button("Prédire")

            # Onglet Requêtes simples (existant)
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
                        choices=interface_manager.annees, label="Année", visible=True
                    )
                    code_departement = gr.Dropdown(
                        choices=interface_manager.departements,
                        label="Département",
                        visible=True,
                    )

                query_button = gr.Button("Exécuter")

            # Onglet Requêtes avancées (existant)
            with gr.Tab("Requêtes avancées"):
                custom_query = gr.Textbox(
                    lines=5,
                    placeholder="Entrez votre requête SQL ici...",
                    label="Requête SQL personnalisée",
                )
                advanced_button = gr.Button("Exécuter")

            # Zone de résultats commune au onglets
            with gr.Column():
                gr.Markdown("## Visualisations")

                # Création dynamique des plots en fonction du service sélectionné
                def update_plots_visibility(service):
                    if service == "Sécurité Immobilière":
                        logger.info("Updating plots visibility for Sécurité Immobilière")
                        return {
                            plot1: gr.update(
                                visible=True, label="P1"
                            ),
                            plot2: gr.update(
                                visible=True, label="P2"
                            ),
                            plot3: gr.update(visible=True),
                            plot4: gr.update(visible=True),
                        }
                    else:
                        return {
                            plot1: gr.update(visible=True, label="P1"),
                            plot2: gr.update(
                                visible=True, label="P2"
                            ),
                            plot3: gr.update(
                                visible=False, label="P3"
                            ),
                            plot4: gr.update(visible=False, label="P4"),
                        }

                with gr.Row():
                    plot1 = gr.Plot(label="P1")
                    plot2 = gr.Plot(label="P2")
                with gr.Row():
                    plot3 = gr.Plot(label="P3", visible=False)
                    plot4 = gr.Plot(label="P4", visible=False)

                # Connexion de l'événement de changement de service
                security_service.change(
                    fn=update_plots_visibility,
                    inputs=[security_service],
                    outputs=[plot1, plot2, plot3, plot4],
                )

                gr.Markdown("## Analyse et Recommandations")
                insights = gr.Textbox(lines=3)

                gr.Markdown("## Résultats détaillés")
                output_df = gr.DataFrame(label="Résultats", interactive=False)

            # Fonction pour gérer la visibilité des champs
            def update_security_fields(service):
                if service == "TransportSécurité":
                    return {
                        dept_security: gr.update(visible=True, label="Département de départ"),
                        dept_security_dest: gr.update(visible=True),
                        year_security: gr.update(visible=True)
                    }
                elif service == "AlerteVoisinage":
                    return {
                        dept_security: gr.update(visible=True, label="Département"),
                        dept_security_dest: gr.update(visible=False),
                        year_security: gr.update(visible=True)
                    }
                elif service == "BusinessSecurity":
                    return {
                        dept_security: gr.update(visible=True, label="Département"),
                        dept_security_dest: gr.update(visible=False),
                        year_security: gr.update(visible=True)
                    }
                else:
                    return {
                        dept_security: gr.update(visible=True, label="Département"),
                        dept_security_dest: gr.update(visible=False),
                        year_security: gr.update(visible=True)
                    }

            # Connexion du bouton d'analyse
            security_button.click(
                fn=interface_manager.security_service.process_request,
                inputs=[
                    security_service,
                    dept_security,
                    year_security,
                    dept_security_dest,
                    crime_type_security
                ],
                outputs=[output_df, insights, plot1, plot2, plot3, plot4],
            )

        territorial_button.click(
            fn=interface_manager.territorial_service.process_request,
            inputs=[territorial_service, region_reference, region_comparison],
            outputs=[output_df, insights, plot1, plot2]
        )

        prediction_button.click(
            interface_manager.predictive_service.process_request,
            inputs=[prediction_service, crime_type_pred, dept_pred, horizon_pred],
            outputs=[output_df, insights],
        )

        # Event handlers for query buttons (existing)
        query_button.click(
            interface_manager.execute_predefined_query,
            inputs=[query_dropdown, type_crime, annee, code_departement],
            outputs=[output_df, insights],
        )

        advanced_button.click(
            interface_manager.execute_custom_query,
            inputs=[custom_query],
            outputs=[output_df, insights],
        )

    return interface.launch(
        share=share, server_name=server_name, server_port=server_port
    )
