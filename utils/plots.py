import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from .config import AppConfig

logger = logging.getLogger(__name__)


class PlotManager:
    def __init__(self):
        self.config = AppConfig()

    def create_choropleth(self, df: pd.DataFrame, year: int) -> go.Figure:
        """
        Crée une carte choroplèthe de la France avec les taux de délinquance
        """
        try:
            fig = px.choropleth(
                df,
                geojson="https://france-geojson.gregoiredavid.fr/repo/regions.geojson",
                locations="code_region",
                featureidkey="properties.code",
                color="taux_pour_mille",
                scope="europe",
                **self.config.PLOT_CONFIG["map"],
                color_continuous_scale=self.config.PLOT_CONFIG["map"]["color_scale"],
                labels=self.config.LABELS,
                title=f"Taux de délinquance par région en {year}",
            )

            # Ajustement de la vue sur la France
            fig.update_geos(center=dict(lat=46.2276, lon=2.2137), projection_scale=4)

            return fig

        except Exception as e:
            logger.error(f"Erreur lors de la création de la carte: {e}")
            raise

    def create_time_series(self, df: pd.DataFrame, region: str = None) -> go.Figure:
        """
        Crée un graphique d'évolution temporelle des faits
        """
        try:
            title = f"Évolution temporelle des faits{' - ' + region if region else ''}"

            fig = px.line(
                df,
                x="annee",
                y="faits",
                color="classe" if "classe" in df.columns else None,
                **self.config.PLOT_CONFIG["line"],
                labels=self.config.LABELS,
                title=title,
            )

            return fig

        except Exception as e:
            logger.error(f"Erreur lors de la création du graphique temporel: {e}")
            raise

    def create_comparison_bar(
        self, df: pd.DataFrame, metric: str = "faits"
    ) -> go.Figure:
        """
        Crée un graphique en barres comparatif entre régions
        """
        try:
            fig = px.bar(
                df,
                x="region",
                y=metric,
                color="classe" if "classe" in df.columns else None,
                **self.config.PLOT_CONFIG["bar"],
                labels=self.config.LABELS,
                title=f"Comparaison par région - {self.config.LABELS[metric]}",
            )

            # Rotation des labels pour meilleure lisibilité
            fig.update_layout(xaxis_tickangle=-45)

            return fig

        except Exception as e:
            logger.error(f"Erreur lors de la création du graphique en barres: {e}")
            raise

    def create_correlation_scatter(self, df: pd.DataFrame) -> go.Figure:
        """
        Crée un nuage de points population vs faits
        """
        try:
            fig = px.scatter(
                df,
                x="population",
                y="faits",
                color="region" if "region" in df.columns else None,
                **self.config.PLOT_CONFIG["scatter"],
                labels=self.config.LABELS,
                title="Corrélation Population - Nombre de faits",
                trendline="ols",  # Ajoute une ligne de tendance
            )

            return fig

        except Exception as e:
            logger.error(f"Erreur lors de la création du nuage de points: {e}")
            raise

    def create_category_distribution(self, df: pd.DataFrame) -> go.Figure:
        """
        Crée un graphique de distribution des types de délits
        """
        try:
            # Agrégation par classe
            df_agg = df.groupby("classe")["faits"].sum().reset_index()

            fig = px.pie(
                df_agg,
                values="faits",
                names="classe",
                title="Distribution des types de délits",
                height=500,
                width=700,
            )

            # Ajustement de la mise en page
            fig.update_traces(textposition="inside", textinfo="percent+label")

            return fig

        except Exception as e:
            logger.error(
                f"Erreur lors de la création du graphique de distribution: {e}"
            )
            raise

    def create_heatmap(self, df: pd.DataFrame) -> go.Figure:
        """
        Crée une heatmap des délits par année et région
        """
        try:
            # Pivot de données pour la heatmap
            pivot_data = df.pivot_table(
                values="faits", index="region", columns="annee", aggfunc="sum"
            )

            fig = go.Figure(
                data=go.Heatmap(
                    z=pivot_data.values,
                    x=pivot_data.columns,
                    y=pivot_data.index,
                    colorscale=self.config.PLOT_CONFIG["map"]["color_scale"],
                )
            )

            fig.update_layout(
                title="Heatmap des délits par région et année",
                xaxis_title="Année",
                yaxis_title="Région",
                height=600,
            )

            return fig

        except Exception as e:
            logger.error(f"Erreur lors de la création de la heatmap: {e}")
            raise
