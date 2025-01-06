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
        
        ### PASSAGE A AlerteVoisinage ###
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
        
        ### PASSAGE A Buisiness Security ###
    def create_business_impact_heatmap(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une heatmap de l'impact des crimes sur différents types d'activités commerciales"""
        try:
            required_columns = ['type_crime', 'taux_dept', 'niveau_risque']
            if not self._validate_dataframe(df, required_columns):
                return None

            # Conversion des données en float
            df_clean = df.copy()
            df_clean['taux_dept'] = df_clean['taux_dept'].astype(float)

            # Définition des types d'activités commerciales et leur sensibilité aux différents crimes
            business_types = {
                'Commerce de détail': {'Vols': 0.9, 'Cambriolages': 0.8, 'Destructions': 0.6},
                'Restauration': {'Vols': 0.7, 'Cambriolages': 0.6, 'Destructions': 0.8},
                'Services financiers': {'Vols': 1.0, 'Cambriolages': 0.9, 'Destructions': 0.4},
                'Grande distribution': {'Vols': 0.8, 'Cambriolages': 0.7, 'Destructions': 0.5},
                'Commerce de luxe': {'Vols': 1.0, 'Cambriolages': 1.0, 'Destructions': 0.7}
            }

            # Création de la matrice d'impact
            impact_matrix = []
            business_labels = list(business_types.keys())
            crime_categories = ['Vols', 'Cambriolages', 'Destructions']

            for business in business_labels:
                row = []
                for crime in crime_categories:
                    # Calcul de l'impact en tenant compte du taux et de la sensibilité
                    taux_moyen = float(df_clean[df_clean['type_crime'].str.contains(crime, case=False)]['taux_dept'].mean())
                    sensibilite = business_types[business][crime]
                    impact = taux_moyen * sensibilite
                    row.append(impact)
                impact_matrix.append(row)

            # Création de la heatmap avec une colorscale personnalisée
            colorscale = [
                [0.0, '#198754'],      # Vert pour impact très faible
                [0.25, '#90EE90'],     # Vert clair pour impact faible
                [0.5, '#ffc107'],      # Jaune pour impact moyen
                [0.75, '#ff7f50'],     # Orange pour impact élevé
                [1.0, '#dc3545']       # Rouge pour impact très élevé
            ]

            # Création de la heatmap
            fig = go.Figure(data=go.Heatmap(
                z=impact_matrix,
                x=crime_categories,
                y=business_labels,
                colorscale=colorscale,
                colorbar=dict(
                    title="Indice d'impact",
                    titleside="right",
                    ticktext=["Très faible", "Faible", "Moyen", "Élevé", "Très élevé"],
                    tickvals=[min(min(row) for row in impact_matrix),
                            min(min(row) for row in impact_matrix) + (max(max(row) for row in impact_matrix) - min(min(row) for row in impact_matrix))*0.25,
                            min(min(row) for row in impact_matrix) + (max(max(row) for row in impact_matrix) - min(min(row) for row in impact_matrix))*0.5,
                            min(min(row) for row in impact_matrix) + (max(max(row) for row in impact_matrix) - min(min(row) for row in impact_matrix))*0.75,
                            max(max(row) for row in impact_matrix)],
                    tickmode="array",
                    thickness=5,
                    len=0.75,
                    y=0.5
                ),
                hoverongaps=False,
                hovertemplate=(
                    "<b>Secteur: %{y}</b><br>" +
                    "<b>Type de crime: %{x}</b><br>" +
                    "Indice d'impact: %{z:.2f}<br>" +
                    "<extra></extra>"
                )
            ))

            # Mise en page améliorée
            fig.update_layout(
                title={
                    'text': "Impact des risques par secteur d'activité",
                    'y': 0.95,
                    'x': 0.5,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'font': dict(size=24)
                },
                height=600,
                margin=dict(t=100, l=150, r=150, b=100),
                xaxis=dict(
                    title="Types de crimes",
                    title_font=dict(size=14),
                    tickfont=dict(size=12)
                ),
                yaxis=dict(
                    title="Secteurs d'activité",
                    title_font=dict(size=14),
                    tickfont=dict(size=12)
                )
            )

            # Ajout de la légende explicative
            fig.add_annotation(
                text=(
                    "<b>Guide de lecture :</b><br>" +
                    "• L'indice d'impact combine deux facteurs :<br>" +
                    "  1. Le taux d'incidents dans la zone<br>" +
                    "  2. La vulnérabilité spécifique du secteur<br>" +
                    "• Plus la couleur tend vers le rouge,<br>" +
                    "  plus l'impact est important"
                ),
                xref="paper", yref="paper",
                x=-0.5,
                y=-0.25,
                showarrow=False,
                font=dict(size=12),
                align="left",
                bgcolor="rgba(255, 255, 255, 0.0)",
                borderpad=4
            )

            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création de la heatmap d'impact: {str(e)}")
            logger.exception("Détails complets de l'erreur:")
            return None
    
    def create_business_zone_assessment(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une évaluation des zones commerciales avec indicateurs business"""
        try:
            required_columns = ['type_crime', 'taux_dept', 'niveau_risque', 'annee', 'taux_national']
            if not self._validate_dataframe(df, required_columns):
                return None

            # Sélection des données les plus récentes
            latest_year = df['annee'].max()
            latest_data = df[df['annee'] == latest_year].copy()
            
            # Conversion explicite des colonnes en float
            numeric_cols = ['taux_dept', 'taux_national']
            for col in numeric_cols:
                latest_data[col] = latest_data[col].astype(float)

            # 1. Calcul du Risque commercial
            # Augmentation du coefficient de 6 à 8 pour être plus strict
            taux_moyen = latest_data['taux_dept'].mean()
            risk_score = max(0, 100 - (taux_moyen * 8))

            # 2. Calcul de l'Attractivité zone
            commercial_crimes = latest_data[
                latest_data['type_crime'].str.contains('Vol|Cambrio|Destruction', case=False)
            ]
            
            if not commercial_crimes.empty:
                taux_commercial = commercial_crimes['taux_dept'].mean()
                # Augmentation du coefficient de 6 à 7.5 pour être plus strict
                attractivity_score = max(0, 100 - (taux_commercial * 7.5))
            else:
                attractivity_score = 50

            # 3. Calcul de la Sécurité globale
            # Augmentation des poids pour les crimes graves
            crime_weights = {
                'Vols': 1.5,
                'Cambriolages': 2.5,  # Augmenté de 2.0 à 2.5
                'Destruction': 1.2,    # Augmenté de 1.0 à 1.2
                'Violence': 3.0        # Augmenté de 2.5 à 3.0
            }
            
            weighted_taux = 0
            total_weight = 0
            
            for crime_type, weight in crime_weights.items():
                crimes = latest_data[latest_data['type_crime'].str.contains(crime_type, case=False)]
                if not crimes.empty:
                    weighted_taux += crimes['taux_dept'].mean() * weight
                    total_weight += weight
            
            if total_weight > 0:
                avg_weighted_taux = weighted_taux / total_weight
                # Augmentation du coefficient de 5 à 6.5 pour être plus strict
                security_score = max(0, 100 - (avg_weighted_taux * 6.5))
            else:
                security_score = 50

            # Préparation des métriques pour l'affichage
            business_metrics = {
                'Risque commercial': {
                    'score': risk_score,
                    'color': '#0d6efd',
                    'description': f"Taux moyen: {taux_moyen:.1f}‰"
                },
                'Attractivité zone': {
                    'score': attractivity_score,
                    'color': '#198754',
                    'description': f"Taux des crimes commerciaux: {taux_commercial:.1f}‰"
                },
                'Sécurité globale': {
                    'score': security_score,
                    'color': '#dc3545',
                    'description': f"Taux pondéré: {avg_weighted_taux:.1f}‰"
                }
            }

            # Création du graphique
            fig = go.Figure()

            # Ajout des barres pour chaque métrique
            x_pos = 0
            for metric_name, metric_data in business_metrics.items():
                fig.add_trace(go.Bar(
                    x=[x_pos],
                    y=[metric_data['score']],
                    name=metric_name,
                    marker_color=metric_data['color'],
                    text=[f"{metric_data['score']:.1f}"],
                    textposition='auto',
                    width=0.8,
                    hovertemplate=(
                        f"<b>{metric_name}</b><br>" +
                        f"{metric_data['description']}<br>" +
                        "Score: %{y:.1f}/100<br>" +
                        "<extra></extra>"
                    )
                ))
                x_pos += 1

            # Ajout des seuils
            seuils = [
                {'score': 60, 'color': 'red', 'style': 'dash', 'text': 'Seuil critique'},
                {'score': 75, 'color': 'orange', 'style': 'dash', 'text': 'Seuil de vigilance'},
                {'score': 85, 'color': 'green', 'style': 'dash', 'text': 'Objectif recommandé'}
            ]

            for seuil in seuils:
                fig.add_shape(
                    type='line',
                    x0=-0.5,
                    x1=2.5,
                    y0=seuil['score'],
                    y1=seuil['score'],
                    line=dict(
                        color=seuil['color'],
                        width=2,
                        dash=seuil['style']
                    )
                )
                # Ajout du texte du seuil
                fig.add_annotation(
                    text=f"{seuil['text']}",
                    xref="paper",
                    yref="y",
                    x=1.1,
                    y=seuil['score'],
                    showarrow=False,
                    font=dict(size=12, color=seuil['color'])
                )

            # Mise en page
            fig.update_layout(
                title={
                    'text': "Évaluation des zones d'activité commerciale",
                    'y': 0.95,
                    'x': 0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                xaxis=dict(
                    ticktext=list(business_metrics.keys()),
                    tickvals=list(range(len(business_metrics))),
                    title=""
                ),
                yaxis=dict(
                    title="Score sur 100",
                    range=[0, 100]
                ),
                showlegend=False,
                height=500,
                margin=dict(t=100, l=50, r=200, b=100)  # Augmenté la marge droite pour les annotations
            )

            # Ajout des annotations explicatives pour chaque score
            fig.add_annotation(
                text=(
                    "<b>Interprétation des scores</b><br><br>" +
                    "• > 85 : Zone favorable<br>" +
                    "• 75-85 : Zone à surveiller<br>" +
                    "• 60-75 : Zone sensible<br>" +
                    "• < 60 : Zone critique"
                ),
                xref="paper",
                yref="paper",
                x=1.6,
                y=-0.3,
                showarrow=False,
                font=dict(size=12),
                align="left",
                bgcolor="white",
                bordercolor="black",
                borderwidth=1,
                borderpad=4
            )

            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création de l'évaluation des zones: {str(e)}")
            logger.exception("Détails complets de l'erreur:")
            return None
  
    ### PASSAGE A OptimisationAssurance ###
    def create_insurance_risk_heatmap(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée une heatmap des risques pour les assurances"""
        try:
            required_columns = ['type_crime', 'quintile_risque', 'score_assurance']
            if not self._validate_dataframe(df, required_columns):
                return None
            
            # Préparation des données
            pivot_data = pd.pivot_table(
                df,
                values='score_assurance',
                index='type_crime',
                columns='quintile_risque',
                aggfunc='mean'
            ).round(2)
            
            # Création de la heatmap
            fig = go.Figure(data=go.Heatmap(
                z=pivot_data.values,
                x=['Très faible', 'Faible', 'Moyen', 'Élevé', 'Très élevé'],
                y=pivot_data.index,
                colorscale=[
                    [0, '#198754'],    # Vert - Risque faible
                    [0.25, '#90EE90'],  # Vert clair
                    [0.5, '#ffc107'],   # Jaune
                    [0.75, '#fd7e14'],  # Orange
                    [1, '#dc3545']      # Rouge - Risque élevé
                ],
                hoverongaps=False,
                hovertemplate=(
                    "<b>%{y}</b><br>" +
                    "Niveau de risque: %{x}<br>" +
                    "Score de risque: %{z:.2f}<br>" +
                    "<extra></extra>"
                )
            ))
            
            fig.update_layout(
                title={
                    'text': "Matrice de risque par type de délit",
                    'y': 0.95,
                    'x': 0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                xaxis_title="Niveau de risque",
                yaxis_title="Type de délit",
                height=500,
                margin=dict(l=50, r=50, t=100, b=100)
            )
            
            # Ajout d'une annotation explicative
            fig.add_annotation(
                text=(
                    "Guide de lecture :<br>" +
                    "• Plus la couleur tend vers le rouge, plus le risque est élevé<br>" +
                    "• Score de risque basé sur la fréquence et la gravité des délits"
                ),
                xref="paper", yref="paper",
                x=-1.5, y=-0.2,
                showarrow=False,
                font=dict(size=12),
                align="left",
                bgcolor="rgba(255, 255, 255, 0.8)",
                bordercolor="black",
                borderwidth=1
            )
            
            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création de la heatmap: {str(e)}")
            return None

    def create_insurance_scoring(self, df: pd.DataFrame) -> Optional[go.Figure]:
        """Crée un graphique de scoring pour l'ajustement des primes"""
        try:
            required_columns = ['type_crime', 'quintile_risque', 'indice_relatif']
            if not self._validate_dataframe(df, required_columns):
                return None
            
            # Création des catégories de risque avec impact sur les primes
            risk_categories = {
                1: 'Prime réduite (-20%)',
                2: 'Prime réduite (-10%)',
                3: 'Prime standard',
                4: 'Prime majorée (+10%)',
                5: 'Prime majorée (+20%)'
            }
            
            # Préparation des données
            df_scoring = df.copy()
            df_scoring['ajustement_prime'] = df_scoring['quintile_risque'].map(risk_categories)
            
            # Création du scatter plot
            fig = go.Figure()
            
            # Ajout d'une ligne de référence pour la moyenne nationale
            fig.add_hline(
                y=100, 
                line_dash="dash",
                line_color="gray",
                annotation_text="Moyenne nationale",
                annotation_position="right"
            )
            
            # Définition des couleurs par niveau de prime
            colors = {
                'Prime réduite (-20%)': '#198754',
                'Prime réduite (-10%)': '#90EE90',
                'Prime standard': '#ffc107',
                'Prime majorée (+10%)': '#fd7e14',
                'Prime majorée (+20%)': '#dc3545'
            }
            
            # Ajout des points pour chaque niveau de prime
            for prime_cat in risk_categories.values():
                mask = df_scoring['ajustement_prime'] == prime_cat
                fig.add_trace(go.Scatter(
                    x=df_scoring[mask]['type_crime'],
                    y=df_scoring[mask]['indice_relatif'],
                    mode='markers',
                    name=prime_cat,
                    marker=dict(
                        size=15,
                        color=colors[prime_cat],
                        symbol='circle'
                    ),
                    hovertemplate=(
                        "<b>%{x}</b><br>" +
                        "Indice de risque: %{y:.1f}%<br>" +
                        f"Ajustement: {prime_cat}<br>" +
                        "<extra></extra>"
                    )
                ))
            
            fig.update_layout(
                title={
                    'text': "Scoring territorial et ajustement des primes",
                    'y': 0.95,
                    'x': 0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                xaxis_title="Type de délit",
                yaxis_title="Indice de risque (%)",
                height=500,
                showlegend=True,
                legend=dict(
                    title="Recommandation tarifaire",
                    yanchor="top",
                    y=0.95,
                    xanchor="left",
                    x=1.15,
                    bgcolor="rgba(255, 255, 255, 0.8)",
                    bordercolor="black",
                    borderwidth=1
                ),
                margin=dict(l=50, r=150, t=100, b=100)
            )
            
            # Rotation des labels sur l'axe x
            fig.update_xaxes(tickangle=45)
            
            return fig
        except Exception as e:
            logger.error(f"Erreur lors de la création du scoring: {str(e)}")
            return None