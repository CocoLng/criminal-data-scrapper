import logging
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go

logger = logging.getLogger(__name__)


class PredictiveVisualization:
    """Classe gérant toutes les visualisations liées à l'analyse prédictive"""

    def __init__(self):
        self.color_scale = [[0, "#198754"], [0.5, "#ffc107"], [1, "#dc3545"]]

    def _validate_dataframe(self, df: pd.DataFrame, required_columns: list) -> bool:
        """Valide si le DataFrame contient les colonnes requises"""
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Colonnes manquantes: {missing_columns}")
            logger.error(f"Colonnes disponibles: {df.columns.tolist()}")
            return False
        return True

    def create_projection_curve(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une courbe d'évolution avec projections futures"""
        try:
            required_columns = [
                "type_crime",
                "annee",
                "projection",
                "lower_bound",
                "upper_bound",
                "data_type",
            ]
            if not self._validate_dataframe(df, required_columns):
                return None

            fig = go.Figure()

            # Pour chaque type de crime
            for crime in df["type_crime"].unique():
                crime_data = df[df["type_crime"] == crime]

                # Données historiques
                historical = crime_data[crime_data["data_type"] == "HISTORIQUE"]
                fig.add_trace(
                    go.Scatter(
                        x=historical["annee"],
                        y=historical["projection"],
                        name=f"{crime} (Historique)",
                        mode="lines+markers",
                        line=dict(width=2),
                        hovertemplate=(
                            "Année: %{x}<br>"
                            + "Taux: %{y:.2f}‰<br>"
                            + "<extra></extra>"
                        ),
                    )
                )

                # Données projetées
                projected = crime_data[crime_data["data_type"] == "PROJECTION"]
                fig.add_trace(
                    go.Scatter(
                        x=projected["annee"],
                        y=projected["projection"],
                        name=f"{crime} (Projection)",
                        mode="lines+markers",
                        line=dict(dash="dash"),
                        hovertemplate=(
                            "Année: %{x}<br>"
                            + "Projection: %{y:.2f}‰<br>"
                            + "<extra></extra>"
                        ),
                    )
                )

                # Intervalle de confiance
                fig.add_trace(
                    go.Scatter(
                        x=projected["annee"].tolist()
                        + projected["annee"].tolist()[::-1],
                        y=projected["upper_bound"].tolist()
                        + projected["lower_bound"].tolist()[::-1],
                        fill="toself",
                        fillcolor="rgba(0,176,246,0.2)",
                        line=dict(color="rgba(255,255,255,0)"),
                        name=f"{crime} (Intervalle de confiance)",
                        showlegend=True,
                        hovertemplate=(
                            "Année: %{x}<br>"
                            + "Intervalle: [%{y:.2f}‰]<br>"
                            + "<extra></extra>"
                        ),
                    )
                )

            fig.update_layout(
                title="Évolution et Projection des Taux de Criminalité",
                xaxis_title="Année",
                yaxis_title="Taux pour 1000 habitants (‰)",
                height=600,
                showlegend=True,
                hovermode="x unified",
                legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
            )

            # Ligne verticale pour séparer historique/projection
            first_projection_year = df[df["data_type"] == "PROJECTION"]["annee"].min()
            fig.add_vline(
                x=first_projection_year - 0.5,
                line_dash="dash",
                line_color="gray",
                annotation_text="Début Projection",
                annotation_position="bottom right",
            )

            return fig

        except Exception as e:
            logger.error(
                f"Erreur lors de la création de la courbe de projection: {str(e)}"
            )
            return None

    def create_prediction_heatmap(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une heatmap des variations prédites"""
        try:
            required_columns = ["type_crime", "annee", "projection", "data_type"]
            if not self._validate_dataframe(df, required_columns):
                return None

            # Calcul des variations par rapport à la dernière année historique
            df_pivot = df.pivot(
                index="type_crime", columns="annee", values="projection"
            )

            # Dernière année historique comme référence
            last_historical = df[df["data_type"] == "HISTORIQUE"]["annee"].max()
            reference_values = df_pivot[last_historical]

            # Calcul des variations en pourcentage
            variations = pd.DataFrame()
            for year in df_pivot.columns[df_pivot.columns > last_historical]:
                variations[year] = (
                    (df_pivot[year] - reference_values) / reference_values * 100
                )

            # Création de la heatmap
            fig = go.Figure(
                data=go.Heatmap(
                    z=variations.values,
                    x=variations.columns,
                    y=variations.index,
                    colorscale=self.color_scale,
                    zmid=0,  # Centre la couleur sur 0
                    colorbar=dict(title="Variation (%)", titleside="right"),
                    hovertemplate=(
                        "Année: %{x}<br>"
                        + "Type: %{y}<br>"
                        + "Variation: %{z:.1f}%<br>"
                        + "<extra></extra>"
                    ),
                )
            )

            fig.update_layout(
                title="Heatmap des Variations Prédites par Type de Crime",
                xaxis_title="Année de projection",
                yaxis_title="Type de crime",
                height=600,
            )

            return fig

        except Exception as e:
            logger.error(f"Erreur lors de la création de la heatmap: {str(e)}")
            return None

    def create_risk_variations(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée un graphique des variations de risque projetées"""
        try:
            required_columns = ["type_crime", "variation_projetee", "tendance"]
            if not self._validate_dataframe(df, required_columns):
                return None

            # Création d'une palette de couleurs basée sur la tendance
            colors = {
                "FORTE_HAUSSE": "#dc3545",
                "HAUSSE_MODEREE": "#ffc107",
                "STABLE": "#6c757d",
                "BAISSE_MODEREE": "#0dcaf0",
                "FORTE_BAISSE": "#198754",
            }

            # Tri par variation projetée
            df_sorted = df.sort_values("variation_projetee")

            fig = go.Figure()

            # Ajout des barres
            fig.add_trace(
                go.Bar(
                    x=df_sorted["type_crime"],
                    y=df_sorted["variation_projetee"],
                    marker_color=[colors[t] for t in df_sorted["tendance"]],
                    hovertemplate=(
                        "Type: %{x}<br>"
                        + "Variation projetée: %{y:.1f}%<br>"
                        + "<extra></extra>"
                    ),
                )
            )

            # Ligne horizontale à 0%
            fig.add_hline(
                y=0, line_dash="dash", line_color="gray", annotation_text="Référence"
            )

            fig.update_layout(
                title="Variations Projetées par Type de Crime",
                xaxis_title="Type de crime",
                yaxis_title="Variation projetée (%)",
                height=600,
                showlegend=False,
                xaxis_tickangle=45,
            )

            return fig

        except Exception as e:
            logger.error(
                f"Erreur lors de la création du graphique des variations: {str(e)}"
            )
            return None

    def create_crime_correlations(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une matrice de corrélation entre les types de crimes"""
        try:
            required_columns = ["type_crime", "type_crime_2", "correlation"]
            if not self._validate_dataframe(df, required_columns):
                return None

            # Récupération de la liste unique des types de crimes
            crimes_uniques = sorted(
                list(set(df["type_crime"].unique()) | set(df["type_crime_2"].unique()))
            )

            # Création d'une matrice vide
            correlation_matrix = np.zeros((len(crimes_uniques), len(crimes_uniques)))

            # Remplissage de la matrice
            crime_to_index = {crime: i for i, crime in enumerate(crimes_uniques)}
            for _, row in df.iterrows():
                i = crime_to_index[row["type_crime"]]
                j = crime_to_index[row["type_crime_2"]]
                correlation_matrix[i, j] = row["correlation"]
                correlation_matrix[j, i] = row["correlation"]  # Symétrie

            # Création de la heatmap
            fig = go.Figure(
                data=go.Heatmap(
                    z=correlation_matrix,
                    x=crimes_uniques,
                    y=crimes_uniques,
                    colorscale="RdBu",
                    zmid=0,
                    colorbar=dict(title="Corrélation", titleside="right"),
                    hovertemplate=(
                        "Type 1: %{x}<br>"
                        + "Type 2: %{y}<br>"
                        + "Corrélation: %{z:.2f}<br>"
                        + "<extra></extra>"
                    ),
                )
            )

            fig.update_layout(
                title="Matrice de Corrélation entre Types de Crimes",
                xaxis_title="Type de crime",
                yaxis_title="Type de crime",
                height=600,
                xaxis_tickangle=45,
            )

            return fig

        except Exception as e:
            logger.error(
                f"Erreur lors de la création de la matrice de corrélation: {str(e)}"
            )
            return None
