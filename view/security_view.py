import plotly.graph_objects as go
from typing import Dict, Optional, List
import pandas as pd
import numpy as np
import logging
import gradio as gr

logger = logging.getLogger(__name__)

class SecurityVisualization:
    """Classe gérant toutes les visualisations liées à la sécurité"""
    
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
    
    def create_risk_gauge(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une jauge montrant le score de sécurité relatif"""
        try:
            required_columns = ['score_securite']
            if not self._validate_dataframe(df, required_columns):
                return None
            
            score_moyen = df['score_securite'].mean()
            
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=score_moyen,
                title={
                    'text': "Score de sécurité relatif<br><span style='font-size:0.8em;color:gray'>Par rapport à la moyenne nationale</span>", 
                    'font': {'size': 24}
                },
                number={
                    'suffix': '%', 
                    'font': {'size': 26},
                    'valueformat': '.1f'
                },
                gauge={
                    'axis': {
                        'range': [-100, 100],
                        'tickwidth': 1,
                        'tickcolor': "darkblue",
                        'ticktext': ['Risqué', 'Modéré', 'Moyenne nationale', 'Sûr', 'Très sûr'],
                        'tickvals': [-80, -40, 0, 40, 80]
                    },
                    'bar': {'color': "#0d6efd"},
                    'bgcolor': "white",
                    'borderwidth': 2,
                    'bordercolor': "gray",
                    'steps': [
                        {'range': [-100, -60], 'color': "#dc3545"},
                        {'range': [-60, -20], 'color': "#ffc107"},
                        {'range': [-20, 20], 'color': "#6c757d"},
                        {'range': [20, 60], 'color': "#198754"},
                        {'range': [60, 100], 'color': "#0d6efd"}
                    ],
                    'threshold': {
                        'line': {'color': "black", 'width': 4},
                        'thickness': 0.75,
                        'value': score_moyen
                    }
                }
            ))
            
            fig.add_annotation(
                text=(
                    "Score calculé à partir du taux d'incidents pour 1000 habitants<br>"
                    "0% = équivalent à la moyenne nationale<br>"
                    "Score positif = plus sûr que la moyenne<br>"
                    "Score négatif = moins sûr que la moyenne"
                ),
                xref="paper", yref="paper",
                x=0, y=-0.3,  # Déplacé plus bas
                showarrow=False,
                font=dict(size=12, color="gray"),
                align="left"
            )
            
            fig.update_layout(height=450)  # Augmenté pour accommoder le texte
            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création de la jauge: {str(e)}")
            return None

    def create_risk_radar(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée un graphique radar des risques"""
        try:
            required_columns = ['type_crime', 'taux_dept', 'taux_national']
            if not self._validate_dataframe(df, required_columns):
                return None
            
            # Calcul des moyennes par type de crime
            dept_rates = df.groupby('type_crime')['taux_dept'].mean()
            national_rates = df.groupby('type_crime')['taux_national'].mean()
            
            fig = go.Figure()
            
            # Moyenne nationale
            fig.add_trace(go.Scatterpolar(
                r=national_rates.values,
                theta=national_rates.index,
                fill='toself',
                name='Moyenne nationale',
                line=dict(color='gray', width=1),
                fillcolor='rgba(128, 128, 128, 0.2)'
            ))
            
            # Données du département
            fig.add_trace(go.Scatterpolar(
                r=dept_rates.values,
                theta=dept_rates.index,
                fill='toself',
                name='Département',
                line=dict(color='#0d6efd', width=2),
                fillcolor='rgba(13, 110, 253, 0.3)'
            ))
            
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        title='',  # Supprimé le titre sur le graphique
                        tickfont=dict(size=8),
                    )),
                showlegend=True,
                title='Distribution des risques par type de crime',
                height=450,  # Augmenté pour accommoder l'annotation
                legend=dict(
                    yanchor="top",
                    y=1.1,
                    xanchor="left",
                    x=0
                )
            )
            
            # Ajout de l'annotation en bas
            fig.add_annotation(
                text='Taux pour 1000 habitants',
                xref="paper", yref="paper",
                x=0.5, y=-0.2,
                showarrow=False,
                font=dict(size=12, color="gray"),
                align="center"
            )
            
            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création du radar: {str(e)}")
            return None

    def create_comparative_analysis(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une analyse comparative avec histogrammes"""
        try:
            required_columns = ['type_crime', 'taux_dept', 'taux_national']
            if not self._validate_dataframe(df, required_columns):
                return None

            # Agrégation des données par type de crime
            analysis_data = df.groupby('type_crime').agg({
                'taux_dept': ['mean', 'std'],
                'taux_national': 'mean'
            }).round(3)

            analysis_data.columns = ['taux_moyen', 'taux_std', 'taux_national']
            analysis_data = analysis_data.sort_values('taux_moyen', ascending=True)

            fig = go.Figure()

            # Barres pour le département
            fig.add_trace(go.Bar(
                name='Département',
                y=analysis_data.index,
                x=analysis_data['taux_moyen'],
                orientation='h',
                marker_color='#0d6efd',
                error_x=dict(
                    type='data',
                    array=analysis_data['taux_std'],
                    visible=True,
                    color='#0d6efd',
                    thickness=1.5,
                    width=3
                )
            ))

            # Points pour la moyenne nationale
            fig.add_trace(go.Scatter(
                name='Moyenne nationale',
                y=analysis_data.index,
                x=analysis_data['taux_national'],
                mode='markers',
                marker=dict(
                    symbol='diamond',
                    size=10,
                    color='red'
                )
            ))

            fig.update_layout(
                title='Analyse comparative des taux de criminalité',
                xaxis_title='Taux pour 1000 habitants',
                yaxis=dict(
                    title='Type de crime',
                    categoryorder='total ascending'
                ),
                height=500,
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.2,
                    xanchor="right",
                    x=1
                ),
                barmode='group',
                margin=dict(l=50, r=50, t=50, b=100)
            )
            
            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création de l'analyse comparative: {str(e)}")
            return None
        
    def generate_security_visualizations(self, df: pd.DataFrame) -> List[go.Figure]:
        """Génère toutes les visualisations de sécurité"""
        try:
            figures = []
            
            # Génération de la jauge de risque global
            gauge = self.create_risk_gauge(df)
            if gauge:
                figures.append(gauge)
                
            # Génération du radar des risques
            radar = self.create_risk_radar(df)
            if radar:
                figures.append(radar)
                
            # Génération de la distribution des risques (nouveau)
            risk_dist = self.create_risk_distribution(df)
            if risk_dist:
                figures.append(risk_dist)
                
            # Génération de l'analyse comparative
            comparative = self.create_comparative_analysis(df)
            if comparative:
                figures.append(comparative)
            
            return figures
            
        except Exception as e:
            logger.error(f"Erreur lors de la génération des visualisations: {str(e)}")
            return []

    def create_risk_distribution(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une visualisation de la distribution des risques par type de crime pour l'année"""
        try:
            required_columns = ['type_crime', 'taux_dept', 'niveau_risque']
            if not self._validate_dataframe(df, required_columns):
                return None
                
            # Préparation des données
            risk_data = df.groupby(['type_crime', 'niveau_risque'])['taux_dept'].mean().reset_index()
            
            # Définition des couleurs par niveau de risque
            color_map = {
                'ÉLEVÉ': '#dc3545',    # Rouge
                'MODÉRÉ': '#ffc107',   # Jaune
                'FAIBLE': '#198754'    # Vert
            }
            
            # Création du graphique
            fig = go.Figure()
            
            # Ajout des barres pour chaque type de crime
            for risk_level in ['FAIBLE', 'MODÉRÉ', 'ÉLEVÉ']:
                mask = risk_data['niveau_risque'] == risk_level
                fig.add_trace(go.Bar(
                    name=f'Risque {risk_level}',
                    x=risk_data[mask]['type_crime'],
                    y=risk_data[mask]['taux_dept'],
                    marker_color=color_map[risk_level],
                    hovertemplate="<b>%{x}</b><br>" +
                                "Taux: %{y:.2f} pour 1000 habitants<br>" +
                                f"Niveau: {risk_level}<extra></extra>"
                ))

            # Mise en page
            fig.update_layout(
                title={
                    'text': 'Distribution des risques par type de crime',
                    'x': 0.5,
                    'xanchor': 'center'
                },
                xaxis_title='Type de crime',
                yaxis_title='Taux pour 1000 habitants',
                barmode='group',
                height=500,
                showlegend=True,
                legend=dict(
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.01
                ),
                margin=dict(b=100)
            )

            # Rotation des labels sur l'axe x pour une meilleure lisibilité
            fig.update_xaxes(tickangle=45)
            
            # Ajout d'une annotation explicative
            fig.add_annotation(
                text=(
                    "Distribution des incidents par type de crime et niveau de risque. "
                ),
                xref="paper", yref="paper",
                x=0.5, y=-1.3, 
                showarrow=False,
                font=dict(size=12, color="gray"),
                align="center"
            )
            fig.add_annotation(
                text=(
                    "Les barres représentent le taux d'incidents pour 1000 habitants."
                ),
                xref="paper", yref="paper",
                x=0.5, y=-1.4, 
                showarrow=False,
                font=dict(size=12, color="gray"),
                align="center"
            )
            
            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création de la distribution des risques: {str(e)}")
            return None
        
        ### PASSAGE A AlerteVoisinage+ ###
    def create_alert_heatmap(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une heatmap des niveaux d'alerte par type de crime"""
        try:
            required_columns = ['type_crime', 'z_score', 'niveau_alerte']
            if not self._validate_dataframe(df, required_columns):
                return None

            # Préparation des données
            pivot_data = df.pivot_table(
                values='z_score',
                index='type_crime',
                columns='niveau_alerte',
                aggfunc='count',
                fill_value=0
            )

            # Création de la heatmap
            fig = go.Figure(data=go.Heatmap(
                z=pivot_data.values,
                x=pivot_data.columns,
                y=pivot_data.index,
                colorscale=[
                    [0, '#198754'],    # Vert
                    [0.33, '#ffc107'],  # Jaune
                    [0.66, '#fd7e14'],  # Orange
                    [1, '#dc3545']      # Rouge
                ],
                hoverongaps=False,
                hovertemplate=(
                    "Type: %{y}<br>" +
                    "Niveau: %{x}<br>" +
                    "Nombre: %{z}<br>" +
                    "<extra></extra>"
                )
            ))

            fig.update_layout(
                title='Distribution des alertes par type de crime',
                xaxis_title='Niveau d\'alerte',
                yaxis_title='Type de crime',
                height=400
            )

            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création de la heatmap: {str(e)}")
            return None

    def create_alert_gauge(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une jauge de niveau d'alerte basée sur les z-scores"""
        try:
            required_columns = ['z_score', 'niveau_alerte', 'annee']
            if not self._validate_dataframe(df, required_columns):
                return None

            df_clean = df.dropna(subset=['z_score', 'niveau_alerte'])
            
            if df_clean.empty:
                logger.warning("Aucune donnée valide pour créer la jauge d'alerte")
                return None

            # Calcul du z-score maximal et du niveau correspondant
            max_z_score_idx = df_clean['z_score'].idxmax()
            max_z_score = df_clean.loc[max_z_score_idx, 'z_score']
            niveau_max = df_clean.loc[max_z_score_idx, 'niveau_alerte']

            # Vérification des valeurs
            if pd.isna(max_z_score) or pd.isna(niveau_max):
                logger.warning("Valeurs maximales non valides")
                return None
            
            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=max_z_score,
                title={
                    'text': f"Niveau d'Alerte Maximum<br><span style='font-size:0.8em;color:gray'>{niveau_max}</span>",
                    'font': {'size': 24}
                },
                number={
                    'suffix': 'σ',
                    'font': {'size': 26},
                    'valueformat': '.1f'
                },
                delta={'reference': 2, 'decreasing': {'color': "#198754"}, 'increasing': {'color': "#dc3545"}},
                gauge={
                    'axis': {
                        'range': [0, 4],
                        'tickwidth': 1,
                        'tickcolor': "darkblue",
                        'ticktext': ['NORMAL', 'VIGILANCE', 'ALERTE', 'ALERTE ROUGE'],
                        'tickvals': [0, 1, 2, 3]
                    },
                    'bar': {'color': "darkblue"},
                    'bgcolor': "white",
                    'borderwidth': 2,
                    'bordercolor': "gray",
                    'steps': [
                        {'range': [0, 1], 'color': "#198754"},  # Vert
                        {'range': [1, 2], 'color': "#ffc107"},  # Jaune
                        {'range': [2, 3], 'color': "#fd7e14"},  # Orange
                        {'range': [3, 4], 'color': "#dc3545"}   # Rouge
                    ],
                    'threshold': {
                        'line': {'color': "black", 'width': 4},
                        'thickness': 0.75,
                        'value': max_z_score
                    }
                }
            ))

            fig.update_layout(height=300)
            
            fig.add_annotation(
                text=(
                    "σ = écart-type par rapport à la normale<br>"
                    "Seuils : < 1σ Normal, 1-2σ Vigilance, 2-3σ Alerte, > 3σ Alerte Rouge"
                ),
                xref="paper", yref="paper",
                x=0, y=-0.3,
                showarrow=False,
                font=dict(size=12, color="gray"),
                align="left"
            )

            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création de la jauge d'alerte: {str(e)}")
            return None