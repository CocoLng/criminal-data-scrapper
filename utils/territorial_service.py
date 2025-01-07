import logging
from typing import Tuple

import gradio as gr
import pandas as pd

from database.database import DatabaseConnection
from view.territorial_view import TerritorialVisualization

logger = logging.getLogger(__name__)


class TerritorialService:
    def __init__(self):
        self.db = DatabaseConnection()
        self.visualizer = TerritorialVisualization()

    def process_request(
        self, service: str, region_ref: str, region_comp: str = None, **kwargs
    ) -> Tuple[pd.DataFrame, str, gr.Plot, gr.Plot]:
        """Traite les requêtes d'analyse territoriale"""
        try:
            empty_plots = [gr.Plot(), gr.Plot()]

            if service == "Diagnostic Régional":
                df, recommendations = self._diagnostic_regional(region_ref)
                if df.empty:
                    return df, recommendations, *empty_plots

                plots = empty_plots
                # Création de la heatmap régionale
                heatmap = self.visualizer.create_regional_heatmap(df)
                if heatmap is not None:
                    plots[0] = gr.Plot(heatmap)

                # Création du radar régional
                radar = self.visualizer.create_regional_radar(df)
                if radar is not None:
                    plots[1] = gr.Plot(radar)

                return df, recommendations, *plots

            elif service == "Comparaison Inter-Régionale":
                if not region_comp:
                    return (
                        pd.DataFrame(),
                        "Veuillez sélectionner une région à comparer",
                        *empty_plots,
                    )

                # [Le reste du code existant pour la comparaison reste inchangé]
                df, recommendations = self._comparaison_interregionale(
                    region_ref, region_comp
                )
                if df.empty:
                    return df, recommendations, *empty_plots

                plots = empty_plots
                bars = self.visualizer.create_interregional_bars(df)
                if bars is not None:
                    plots[0] = gr.Plot(bars)

                boxplot = self.visualizer.create_interregional_boxplot(df)
                if boxplot is not None:
                    plots[1] = gr.Plot(boxplot)

                return df, recommendations, *plots

            elif service == "Évolution Régionale":
                # [Le reste du code existant pour l'évolution reste inchangé]
                df, recommendations = self._evolution_regionale(region_ref)
                if df.empty:
                    return df, recommendations, *empty_plots

                plots = empty_plots
                evolution = self.visualizer.create_temporal_evolution(df)
                if evolution is not None:
                    plots[0] = gr.Plot(evolution)

                heatmap = self.visualizer.create_temporal_heatmap(df)
                if heatmap is not None:
                    plots[1] = gr.Plot(heatmap)

                return df, recommendations, *plots

            else:
                return pd.DataFrame(), "Service non reconnu", *empty_plots

        except Exception as e:
            logger.error(f"Erreur dans process_request: {str(e)}")
            logger.exception("Détails de l'erreur:")
            return pd.DataFrame(), f"Erreur: {str(e)}", *empty_plots

    def _diagnostic_regional(self, region: str) -> Tuple[pd.DataFrame, str]:
        """Analyse les départements au sein d'une région"""
        query = """
        WITH RegionalStats AS (
            SELECT 
                d.code_departement,
                d.code_region,
                c.type_crime,
                c.annee,
                s.taux_pour_mille,
                -- Calcul des moyennes régionales
                AVG(s.taux_pour_mille) OVER (
                    PARTITION BY d.code_region, c.type_crime, c.annee
                ) as moyenne_regionale,
                -- Calcul des rangs départementaux
                RANK() OVER (
                    PARTITION BY d.code_region, c.type_crime, c.annee
                    ORDER BY s.taux_pour_mille DESC
                ) as rang_departemental,
                -- Nombre total de départements dans la région
                COUNT(*) OVER (
                    PARTITION BY d.code_region, c.type_crime, c.annee
                ) as nb_dept_region
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_region = %s
            AND c.annee = (SELECT MAX(annee) FROM crimes)
        )
        SELECT 
            *,
            ROUND(((taux_pour_mille - moyenne_regionale) / 
                NULLIF(moyenne_regionale, 0) * 100), 2) as ecart_moyenne,
            CASE 
                WHEN taux_pour_mille > moyenne_regionale * 1.5 THEN 'TRÈS ÉLEVÉ'
                WHEN taux_pour_mille > moyenne_regionale * 1.2 THEN 'ÉLEVÉ'
                WHEN taux_pour_mille < moyenne_regionale * 0.8 THEN 'FAIBLE'
                WHEN taux_pour_mille < moyenne_regionale * 0.5 THEN 'TRÈS FAIBLE'
                ELSE 'MOYEN'
            END as niveau_relatif
        FROM RegionalStats
        ORDER BY type_crime, code_departement;
        """

        try:
            df = self.db.execute_query(query, (region,))
            recommendations = self._generate_diagnostic_recommendations(df)
            return df, recommendations

        except Exception as e:
            logger.error(f"Erreur dans _diagnostic_regional: {str(e)}")
            return pd.DataFrame(), "Erreur lors de l'analyse régionale"

    def _comparaison_interregionale(
        self, region_ref: str, region_comp: str
    ) -> Tuple[pd.DataFrame, str]:
        """Compare deux régions spécifiques"""
        query = """
        WITH RegionStats AS (
            SELECT 
                d.code_region,
                d.code_departement,
                c.type_crime,
                c.annee,
                s.taux_pour_mille,
                COUNT(*) OVER (PARTITION BY d.code_region, c.type_crime) as nb_departements,
                AVG(s.taux_pour_mille) OVER (PARTITION BY d.code_region, c.type_crime) as taux_moyen,
                MIN(s.taux_pour_mille) OVER (PARTITION BY d.code_region, c.type_crime) as taux_min,
                MAX(s.taux_pour_mille) OVER (PARTITION BY d.code_region, c.type_crime) as taux_max,
                STDDEV(s.taux_pour_mille) OVER (PARTITION BY d.code_region, c.type_crime) as ecart_type,
                CASE 
                    WHEN d.code_region = %s THEN 'RÉGION_RÉFÉRENCE'
                    ELSE 'RÉGION_COMPARÉE'
                END as type_region
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_region IN (%s, %s)
            AND c.annee = (SELECT MAX(annee) FROM crimes)
        )
        SELECT 
            rs.*,
            ROUND(
                CASE 
                    WHEN rs.type_region = 'RÉGION_RÉFÉRENCE' THEN
                        ((rs.taux_moyen - comp.taux_moyen) / NULLIF(comp.taux_moyen, 0) * 100)
                    ELSE
                        ((comp.taux_moyen - rs.taux_moyen) / NULLIF(rs.taux_moyen, 0) * 100)
                END, 
            2) as ecart_pourcentage
        FROM RegionStats rs
        JOIN (
            SELECT type_crime, taux_moyen
            FROM RegionStats
            WHERE type_region = 'RÉGION_RÉFÉRENCE'
            GROUP BY type_crime, taux_moyen
        ) comp ON rs.type_crime = comp.type_crime
        ORDER BY rs.type_crime, rs.code_region, rs.taux_pour_mille;
        """

        try:
            params = (region_ref, region_ref, region_comp)
            df = self.db.execute_query(query, params)
            recommendations = self._generate_comparison_recommendations(
                df, region_ref, region_comp
            )
            return df, recommendations

        except Exception as e:
            logger.error(f"Erreur dans _comparaison_interregionale: {str(e)}")
            return pd.DataFrame(), "Erreur lors de la comparaison inter-régionale"

    def _evolution_regionale(self, region: str) -> Tuple[pd.DataFrame, str]:
        """Analyse l'évolution temporelle des tendances régionales"""
        query = """
        WITH YearlyStats AS (
            SELECT 
                d.code_region,
                c.type_crime,
                c.annee,
                AVG(s.taux_pour_mille) as taux_moyen,
                MIN(s.taux_pour_mille) as taux_min,
                MAX(s.taux_pour_mille) as taux_max,
                STDDEV(s.taux_pour_mille) as ecart_type,
                LAG(AVG(s.taux_pour_mille)) OVER (
                    PARTITION BY d.code_region, c.type_crime
                    ORDER BY c.annee
                ) as taux_annee_precedente
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_region = %s
            GROUP BY d.code_region, c.type_crime, c.annee
        )
        SELECT 
            *,
            CASE 
                WHEN taux_annee_precedente IS NULL THEN 0
                ELSE ROUND(
                    ((taux_moyen - taux_annee_precedente) / 
                    NULLIF(taux_annee_precedente, 0) * 100), 
                2)
            END as evolution_pourcentage,
            CASE 
                WHEN taux_annee_precedente IS NULL THEN 'ANNÉE INITIALE'
                WHEN ((taux_moyen - taux_annee_precedente) / 
                    NULLIF(taux_annee_precedente, 0) * 100) > 20 THEN 'FORTE HAUSSE'
                WHEN ((taux_moyen - taux_annee_precedente) / 
                    NULLIF(taux_annee_precedente, 0) * 100) > 10 THEN 'HAUSSE'
                WHEN ((taux_moyen - taux_annee_precedente) / 
                    NULLIF(taux_annee_precedente, 0) * 100) < -20 THEN 'FORTE BAISSE'
                WHEN ((taux_annee_precedente) / 
                    NULLIF(taux_annee_precedente, 0) * 100) < -10 THEN 'BAISSE'
                ELSE 'STABLE'
            END as tendance
        FROM YearlyStats
        ORDER BY type_crime, annee;
        """

        try:
            df = self.db.execute_query(query, (region,))
            recommendations = self._generate_evolution_recommendations(df)
            return df, recommendations

        except Exception as e:
            logger.error(f"Erreur dans _evolution_regionale: {str(e)}")
            return pd.DataFrame(), "Erreur lors de l'analyse de l'évolution"

    def _generate_diagnostic_recommendations(self, df: pd.DataFrame) -> str:
        """Génère des recommandations basées sur le diagnostic régional"""
        if df.empty:
            return "Aucune donnée disponible pour l'analyse"

        recommendations = [
            f"📊 Analyse régionale - Région {df['code_region'].iloc[0]} :"
        ]

        # Nombre de départements dans la région
        nb_departements = df["nb_dept_region"].iloc[0]
        recommendations.append(
            f"\nNombre de départements dans la région : {nb_departements}"
        )

        if nb_departements == 1:
            recommendations.extend(
                [
                    "\nCette région ne contient qu'un seul département.",
                    "Impossible de faire une analyse comparative intra-régionale.",
                    f"\nStatistiques du département {df['code_departement'].iloc[0]} :",
                ]
            )

            # Ajout des statistiques par type de crime
            for _, row in df.iterrows():
                recommendations.append(
                    f"- {row['type_crime']}: {row['taux_pour_mille']:.1f}‰"
                )
        else:
            # Analyse des écarts significatifs
            ecarts_importants = df[abs(df["ecart_moyenne"]) > 20]
            if not ecarts_importants.empty:
                recommendations.append("\nDépartements avec écarts significatifs :")
                for _, row in ecarts_importants.iterrows():
                    recommendations.append(
                        f"- {row['code_departement']} ({row['type_crime']}): "
                        f"{row['ecart_moyenne']:+.1f}% vs moyenne régionale"
                    )

            # Analyse des niveaux relatifs
            for niveau in ["TRÈS ÉLEVÉ", "TRÈS FAIBLE"]:
                niveau_data = df[df["niveau_relatif"] == niveau]
                if not niveau_data.empty:
                    recommendations.append(f"\nDépartements de niveau {niveau} :")
                    for _, row in niveau_data.iterrows():
                        recommendations.append(
                            f"- {row['code_departement']} ({row['type_crime']}): "
                            f"{row['taux_pour_mille']:.1f}‰"
                        )

        return "\n".join(recommendations)

    def _generate_comparison_recommendations(
        self, df: pd.DataFrame, region_ref: str, region_comp: str
    ) -> str:
        """Génère des recommandations basées sur la comparaison entre deux régions"""
        if df.empty:
            return "Aucune donnée disponible pour la comparaison"

        recommendations = [
            f"🔄 Analyse comparative : Région {region_ref} vs Région {region_comp}"
        ]

        # Statistiques générales
        ref_data = df[df["type_region"] == "RÉGION_RÉFÉRENCE"]
        if not ref_data.empty:
            recommendations.append("\nNombre de départements :")
            recommendations.append(
                f"- Région {region_ref}: {ref_data['nb_departements'].iloc[0]}"
            )
            recommendations.append(
                f"- Région {region_comp}: {df[df['type_region'] == 'RÉGION_COMPARÉE']['nb_departements'].iloc[0]}"
            )

        # Analyse des écarts significatifs (>20%)
        ecarts_significatifs = ref_data[abs(ref_data["ecart_pourcentage"]) > 20]
        if not ecarts_significatifs.empty:
            recommendations.append("\nDifférences significatives :")
            for _, row in ecarts_significatifs.iterrows():
                signe = "+" if row["ecart_pourcentage"] > 0 else ""
                recommendations.append(
                    f"- {row['type_crime']}: {signe}{row['ecart_pourcentage']:.1f}% "
                    f"(Taux: {row['taux_moyen']:.1f}‰ vs {row['taux_moyen']/(1 + row['ecart_pourcentage']/100):.1f}‰)"
                )

        return "\n".join(recommendations)

    def _generate_evolution_recommendations(self, df: pd.DataFrame) -> str:
        """Génère des recommandations basées sur l'évolution temporelle"""
        if df.empty:
            return "Aucune donnée disponible pour l'analyse temporelle"

        recommendations = ["📈 Analyse de l'évolution régionale :"]

        # Analyse des tendances significatives
        tendances = df[df["tendance"].isin(["FORTE HAUSSE", "FORTE BAISSE"])]
        if not tendances.empty:
            recommendations.append("\nÉvolutions significatives :")
            for _, row in tendances.iterrows():
                recommendations.append(
                    f"- {row['type_crime']} ({row['annee']}): {row['tendance']} "
                    f"({row['evolution_pourcentage']:+.1f}%)"
                )

        return "\n".join(recommendations)
