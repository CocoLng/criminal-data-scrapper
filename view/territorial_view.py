import plotly.graph_objects as go
import pandas as pd
import numpy as np
import logging
import matplotlib.colors
from typing import Optional
from scipy.spatial.distance import pdist, squareform
from sklearn.decomposition import PCA



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
        """Crée un graphique radar comparant les départements représentatifs"""
        try:
            required_columns = ['code_departement', 'type_crime', 'taux_pour_mille']
            if not self._validate_dataframe(df, required_columns):
                return None

            # Création d'une matrice de profils
            pivot_data = df.pivot(
                index='code_departement',
                columns='type_crime',
                values='taux_pour_mille'
            ).fillna(0)

            # Normalisation des profils
            normalized_profiles = (pivot_data - pivot_data.min()) / (pivot_data.max() - pivot_data.min())
            
            # Sélection des départements représentatifs
            n_departments = len(pivot_data)
            selected_depts = []
            
            if n_departments <= 4:
                selected_depts = pivot_data.index.tolist()
            else:
                # 1. Département le plus proche de la moyenne
                mean_profile = normalized_profiles.mean()
                distances_to_mean = ((normalized_profiles - mean_profile) ** 2).sum(axis=1)
                median_dept = distances_to_mean.idxmin()
                selected_depts.append(median_dept)
                
                # 2. Les deux départements les plus extrêmes
                pca = PCA(n_components=1)
                pca_scores = pca.fit_transform(normalized_profiles)
                extreme_depts = normalized_profiles.index[
                    [np.argmin(pca_scores), np.argmax(pca_scores)]
                ].tolist()
                selected_depts.extend(extreme_depts)
                
                # 3. Le département le plus atypique (plus grande distance aux autres)
                distances = squareform(pdist(normalized_profiles))
                remaining_depts = normalized_profiles.index[
                    ~normalized_profiles.index.isin(selected_depts)
                ]
                if len(remaining_depts) > 0:
                    remaining_distances = distances[
                        normalized_profiles.index.get_indexer(remaining_depts)
                    ][:, normalized_profiles.index.get_indexer(selected_depts)]
                    atypical_dept = remaining_depts[np.mean(remaining_distances, axis=1).argmax()]
                    selected_depts.append(atypical_dept)

            # Filtrer les données pour les départements sélectionnés
            df_selected = df[df['code_departement'].isin(selected_depts)]

            # Calcul des facteurs d'amplification pour chaque type de crime
            amplification_factors = {}
            for crime in df_selected['type_crime'].unique():
                crime_data = df_selected[df_selected['type_crime'] == crime]['taux_pour_mille']
                min_val = crime_data.min()
                max_val = crime_data.max()
                if max_val - min_val < 0.1 * max_val:
                    center = (max_val + min_val) / 2
                    amplification_factors[crime] = {
                        'center': center,
                        'factor': 5
                    }

            # Application de l'amplification
            df_amplified = df_selected.copy()
            for crime, factors in amplification_factors.items():
                mask = df_amplified['type_crime'] == crime
                centered_values = df_amplified.loc[mask, 'taux_pour_mille'] - factors['center']
                df_amplified.loc[mask, 'taux_pour_mille'] = factors['center'] + (centered_values * factors['factor'])

            # Création du radar plot
            fig = go.Figure()

            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
            labels = ['Médian', 'Minimum', 'Maximum', 'Atypique']

            # Ajout des traces pour chaque département
            for idx, dept in enumerate(selected_depts):
                dept_data = df_amplified[df_amplified['code_departement'] == dept]
                original_data = df_selected[df_selected['code_departement'] == dept]
                
                # Ajout du premier point à la fin pour fermer la boucle
                r_values = np.append(dept_data['taux_pour_mille'].values, 
                                dept_data['taux_pour_mille'].values[0])
                theta_values = np.append(dept_data['type_crime'].values,
                                    dept_data['type_crime'].values[0])
                original_values = np.append(original_data['taux_pour_mille'].values,
                                        original_data['taux_pour_mille'].values[0])
                
                label = f"{labels[idx]} (Dept {dept})" if len(selected_depts) > 1 else f"Dept {dept}"
                
                fig.add_trace(go.Scatterpolar(
                    r=r_values,
                    theta=theta_values,
                    name=label,
                    fill='toself',
                    fillcolor=f'rgba{tuple(list(matplotlib.colors.to_rgba(colors[idx]))[:-1] + [0.2])}',
                    line=dict(color=colors[idx], width=2),
                    hovertemplate=(
                        "Département: %{text}<br>" +
                        "Type: %{theta}<br>" +
                        "Taux amplifié: %{r:.1f}‰<br>" +
                        "Taux réel: %{customdata:.1f}‰<br>" +
                        "<extra></extra>"
                    ),
                    text=[dept] * len(r_values),
                    customdata=original_values
                ))

            # Mise en page
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        showline=True,
                        showticklabels=True,
                        ticksuffix="‰",
                        tickfont=dict(size=10),
                        gridwidth=0.5,
                        gridcolor='rgba(0,0,0,0.1)'
                    ),
                    angularaxis=dict(
                        tickfont=dict(size=10),
                        rotation=90,
                        direction='clockwise',
                        gridcolor='rgba(0,0,0,0.1)'
                    )
                ),
                showlegend=True,
                legend=dict(
                    yanchor="top",
                    y=1.2,
                    xanchor="left",
                    x=0.1,
                    orientation="h"
                ),
                title=dict(
                    text="Comparaison des départements représentatifs de la région",
                    y=0.95,
                    x=0.5,
                    xanchor='center'
                ),
                height=600,
                margin=dict(t=100, b=50, l=50, r=50)
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
        """Crée un boxplot épuré pour une comparaison claire entre régions"""
        try:
            required_columns = ['code_region', 'type_crime', 'taux_pour_mille', 
                            'type_region', 'code_departement']
            if not self._validate_dataframe(df, required_columns):
                return None

            fig = go.Figure()
            colors = {'RÉGION_RÉFÉRENCE': '#1f77b4', 'RÉGION_COMPARÉE': '#ff7f0e'}
            
            crime_types = df['type_crime'].unique()
            
            for idx, crime_type in enumerate(crime_types):
                crime_data = df[df['type_crime'] == crime_type]
                
                for region_type in ['RÉGION_RÉFÉRENCE', 'RÉGION_COMPARÉE']:
                    region_data = crime_data[crime_data['type_region'] == region_type]
                    label = "Région de référence" if region_type == 'RÉGION_RÉFÉRENCE' else "Région comparée"
                    
                    # N'affiche la légende que pour le premier type de crime
                    show_legend = idx == 0
                    
                    fig.add_trace(go.Box(
                        name=label if show_legend else None,
                        showlegend=show_legend,
                        y=region_data['taux_pour_mille'],
                        x=[crime_type] * len(region_data),
                        boxpoints='outliers',  # Ne montre que les points aberrants
                        marker=dict(
                            color=colors[region_type],
                            size=4,
                            opacity=0.7
                        ),
                        line=dict(
                            color=colors[region_type],
                            width=2
                        ),
                        fillcolor=f'rgba{tuple(list(matplotlib.colors.to_rgba(colors[region_type]))[:-1] + [0.3])}',
                        hovertemplate=(
                            "%{y:.1f}‰<br>" +
                            "<extra></extra>"
                        )
                    ))

            fig.update_layout(
                title={
                    "text": "Comparaison des taux de criminalité entre régions<br>" +
                        "<span style='font-size:11px;color:gray'>La boîte représente les valeurs entre le 1er et 3e quartile<br>" +
                        "Le trait horizontal dans la boîte est la médiane<br>" +
                        "Les lignes verticales montrent les valeurs min/max<br></span>",
                    "x": 0.4,
                    "y": 0.95,
                    "xanchor": "center",
                    "font": {"size": 12}
                },
                yaxis_title="Taux pour 1000 habitants",
                xaxis_title=None,
                height=500,
                showlegend=True,
                plot_bgcolor='white',
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1,
                    xanchor="center",
                    x=0.5,
                    title=None
                ),
                yaxis=dict(
                    gridcolor='rgba(0,0,0,0.1)',
                    zeroline=True,
                    zerolinecolor='rgba(0,0,0,0.2)'
                ),
                margin=dict(l=50, r=50, t=100, b=50)
            )
            
            # Rotation des labels de l'axe x pour une meilleure lisibilité
            fig.update_xaxes(
                tickangle=30,
                showgrid=True,
                gridcolor='rgba(0,0,0,0.1)'
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
