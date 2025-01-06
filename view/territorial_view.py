import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class TerritorialVisualization:
    """Classe gérant toutes les visualisations liées à l'analyse territoriale"""
    
    def __init__(self):
        self.color_scale = [[0, '#198754'], [0.5, '#ffc107'], [1, '#dc3545']]
        
    def _validate_dataframe(self, df: pd.DataFrame, required_columns: list) -> bool:
        """Valide si le DataFrame contient les colonnes requises"""
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Colonnes manquantes: {missing_columns}")
            logger.error(f"Colonnes disponibles: {df.columns.tolist()}")
            return False
        return True

    def create_regional_heatmap(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une heatmap des taux de criminalité par département"""
        try:
            required_columns = ['code_departement', 'type_crime', 'taux_pour_mille']
            if not self._validate_dataframe(df, required_columns):
                return None

            # Pivot des données pour la heatmap
            pivot_data = df.pivot_table(
                values='taux_pour_mille',
                index='code_departement',
                columns='type_crime',
                aggfunc='mean'
            )

            # Création de la heatmap
            fig = go.Figure(data=go.Heatmap(
                z=pivot_data.values,
                x=pivot_data.columns,
                y=pivot_data.index,
                colorscale=self.color_scale,
                colorbar=dict(title="Taux pour 1000 habitants"),
                hoverongaps=False,
                hovertemplate=(
                    "Département: %{y}<br>" +
                    "Type: %{x}<br>" +
                    "Taux: %{z:.1f}‰<br>" +
                    "<extra></extra>"
                )
            ))

            fig.update_layout(
                title="Distribution des taux de criminalité par département",
                xaxis_title="Types de crimes",
                yaxis_title="Départements",
                height=600,
                margin=dict(l=100, r=50, t=100, b=100)
            )

            # Rotation des labels
            fig.update_xaxes(tickangle=45)

            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création de la heatmap régionale: {str(e)}")
            return None

    def create_regional_radar(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée un graphique radar comparant les départements"""
        try:
            required_columns = ['code_departement', 'type_crime', 'taux_pour_mille']
            if not self._validate_dataframe(df, required_columns):
                return None

            fig = go.Figure()

            # Création d'un radar pour chaque département
            for dept in df['code_departement'].unique():
                dept_data = df[df['code_departement'] == dept]
                
                fig.add_trace(go.Scatterpolar(
                    r=dept_data['taux_pour_mille'].values,
                    theta=dept_data['type_crime'].values,
                    name=f'Dept {dept}',
                    fill='toself',
                    line=dict(width=2),
                    hovertemplate=(
                        "Département: %{text}<br>" +
                        "Type: %{theta}<br>" +
                        "Taux: %{r:.1f}‰<br>" +
                        "<extra></extra>"
                    ),
                    text=[dept] * len(dept_data)
                ))

            fig.update_layout(
                title="Comparaison des départements par type de crime",
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        title="Taux pour 1000 habitants",
                        ticksuffix="‰"
                    ),
                    angularaxis=dict(
                        tickangle=45
                    )
                ),
                showlegend=True,
                height=600
            )

            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création du radar régional: {str(e)}")
            return None

    def create_interregional_bars(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée un graphique à barres comparant les moyennes régionales"""
        try:
            required_columns = ['code_region', 'type_crime', 'taux_moyen', 'type_region']
            if not self._validate_dataframe(df, required_columns):
                return None

            fig = go.Figure()

            # Barres pour chaque région
            for region_type in ['RÉGION_RÉFÉRENCE', 'RÉGION_COMPARÉE']:
                region_data = df[df['type_region'] == region_type]
                
                fig.add_trace(go.Bar(
                    name=f"Région {region_data['code_region'].iloc[0]}",
                    x=region_data['type_crime'],
                    y=region_data['taux_moyen'],
                    hovertemplate=(
                        "Type: %{x}<br>" +
                        "Taux moyen: %{y:.1f}‰<br>" +
                        "<extra></extra>"
                    )
                ))

            fig.update_layout(
                title="Comparaison des moyennes régionales par type de crime",
                xaxis_title="Types de crimes",
                yaxis_title="Taux pour 1000 habitants",
                barmode='group',
                height=600,
                showlegend=True,
                xaxis_tickangle=45,
                margin=dict(l=50, r=50, t=100, b=100)
            )

            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création du graphique à barres: {str(e)}")
            return None

    def create_interregional_boxplot(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée un boxplot montrant la distribution des taux par région"""
        try:
            required_columns = ['code_region', 'type_crime', 'taux_min', 'taux_max', 
                              'taux_moyen', 'ecart_type']
            if not self._validate_dataframe(df, required_columns):
                return None

            fig = go.Figure()

            for _, row in df.iterrows():
                q1 = row['taux_moyen'] - row['ecart_type']
                q3 = row['taux_moyen'] + row['ecart_type']
                
                fig.add_trace(go.Box(
                    name=row['type_crime'],
                    y=[row['taux_min'], q1, row['taux_moyen'], q3, row['taux_max']],
                    q1=[q1],
                    median=[row['taux_moyen']],
                    q3=[q3],
                    lowerfence=[row['taux_min']],
                    upperfence=[row['taux_max']],
                    boxpoints=False,
                    hovertemplate=(
                        "Type: %{x}<br>" +
                        "Min: %{lowerfence:.1f}‰<br>" +
                        "Max: %{upperfence:.1f}‰<br>" +
                        "Moyenne: %{median:.1f}‰<br>" +
                        "<extra></extra>"
                    )
                ))

            fig.update_layout(
                title="Distribution des taux de criminalité par type",
                yaxis_title="Taux pour 1000 habitants",
                xaxis_title="Types de crimes",
                showlegend=False,
                height=600,
                xaxis_tickangle=45,
                margin=dict(l=50, r=50, t=100, b=100)
            )

            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création du boxplot: {str(e)}")
            return None

    def create_temporal_evolution(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une ligne temporelle des taux moyens par région"""
        try:
            required_columns = ['code_region', 'type_crime', 'annee', 'taux_moyen']
            if not self._validate_dataframe(df, required_columns):
                return None

            fig = go.Figure()

            # Une ligne pour chaque type de crime
            for crime_type in df['type_crime'].unique():
                crime_data = df[df['type_crime'] == crime_type]
                
                fig.add_trace(go.Scatter(
                    x=crime_data['annee'],
                    y=crime_data['taux_moyen'],
                    name=crime_type,
                    mode='lines+markers',
                    hovertemplate=(
                        "Année: %{x}<br>" +
                        "Taux moyen: %{y:.1f}‰<br>" +
                        "<extra></extra>"
                    )
                ))

            fig.update_layout(
                title="Évolution temporelle des taux de criminalité",
                xaxis_title="Année",
                yaxis_title="Taux pour 1000 habitants",
                height=600,
                showlegend=True,
                legend=dict(
                    yanchor="top",
                    y=-0.2,
                    xanchor="left",
                    x=0
                ),
                margin=dict(l=50, r=50, t=100, b=200)
            )

            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création du graphique d'évolution: {str(e)}")
            return None

    def create_temporal_heatmap(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une heatmap temporelle de l'évolution des crimes"""
        try:
            required_columns = ['type_crime', 'annee', 'taux_moyen']
            if not self._validate_dataframe(df, required_columns):
                return None

            # Pivot des données pour la heatmap
            pivot_data = df.pivot_table(
                values='taux_moyen',
                index='type_crime',
                columns='annee',
                aggfunc='mean'
            )

            fig = go.Figure(data=go.Heatmap(
                z=pivot_data.values,
                x=pivot_data.columns,
                y=pivot_data.index,
                colorscale=self.color_scale,
                colorbar=dict(title="Taux pour 1000 habitants"),
                hoverongaps=False,
                hovertemplate=(
                    "Type: %{y}<br>" +
                    "Année: %{x}<br>" +
                    "Taux: %{z:.1f}‰<br>" +
                    "<extra></extra>"
                )
            ))

            fig.update_layout(
                title="Évolution des types de crimes au fil du temps",
                xaxis_title="Année",
                yaxis_title="Types de crimes",
                height=600,
                margin=dict(l=100, r=50, t=100, b=50)
            )

            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création de la heatmap temporelle: {str(e)}")
            return None
