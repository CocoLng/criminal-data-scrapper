import pandas as pd
from typing import Tuple
from database.database import DatabaseConnection
from view.predictive_view import PredictiveVisualization
import logging
import gradio as gr

logger = logging.getLogger(__name__)

class PredictiveService:
    def __init__(self):
        self.db = DatabaseConnection()
        self.visualizer = PredictiveVisualization()
        
    def process_request(
        self,
        service: str,
        dept: str = None,
        crime_type: str = None,
        target_year: int = 25
    ) -> Tuple[pd.DataFrame, str, gr.Plot, gr.Plot]:
        """Traite les requêtes d'analyse prédictive"""
        try:
            empty_plots = [gr.Plot(), gr.Plot()]
            
            if service == "Projection Criminelle":
                # Vérification des paramètres
                if not dept:
                    return pd.DataFrame(), "Erreur: Département requis", *empty_plots
                    
                # Conversion et validation de l'année cible
                try:
                    target_year = int(target_year)
                    if target_year < 24 or target_year > 30:
                        return pd.DataFrame(), "Erreur: L'année de prédiction doit être entre 24 et 30", *empty_plots
                except ValueError:
                    return pd.DataFrame(), "Erreur: Année invalide", *empty_plots
                    
                df, recommendations = self._projection_criminelle(dept, crime_type, target_year)
                if df.empty:
                    return df, recommendations, *empty_plots
                    
                plots = empty_plots
                # Création de la courbe de projection
                projection = self.visualizer.create_projection_curve(df)
                if projection is not None:
                    plots[0] = gr.Plot(projection)
                
                # Création de la heatmap des prédictions
                heatmap = self.visualizer.create_prediction_heatmap(df)
                if heatmap is not None:
                    plots[1] = gr.Plot(heatmap)
                    
                return df, recommendations, *plots
                
            elif service == "Analyse des Risques Émergents":
                if not dept:
                    return pd.DataFrame(), "Erreur: Département requis", *empty_plots
                    
                df, recommendations = self._analyse_risques(dept)
                if df.empty:
                    return df, recommendations, *empty_plots
                    
                plots = empty_plots
                variations = self.visualizer.create_risk_variations(df)
                if variations is not None:
                    plots[0] = gr.Plot(variations)
                
                correlations = self.visualizer.create_crime_correlations(df)
                if correlations is not None:
                    plots[1] = gr.Plot(correlations)
                    
                return df, recommendations, *plots
                
            else:
                return pd.DataFrame(), "Service non reconnu", *empty_plots
                
        except Exception as e:
            logger.error(f"Erreur dans process_request: {str(e)}")
            return pd.DataFrame(), f"Erreur: {str(e)}", *empty_plots

    def _projection_criminelle(self, department: str, crime_type: str = None, target_year: int = 25) -> Tuple[pd.DataFrame, str]:
        """Analyse et projette l'évolution des crimes jusqu'à l'année cible"""
        query = """
        WITH RECURSIVE Annees AS (
            -- Sélection de la plus petite année dans les données
            SELECT MIN(c.annee) as annee
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            WHERE s.code_departement = %s
            AND (c.type_crime = %s OR %s IS NULL)
            
            UNION ALL
            
            SELECT annee + 1
            FROM Annees
            WHERE annee < %s  -- Utilisation de l'année cible
        ),
        BaseData AS (
            SELECT 
                s.code_departement,
                c.type_crime,
                c.annee,
                s.taux_pour_mille
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            WHERE s.code_departement = %s
            AND (c.type_crime = %s OR %s IS NULL)
        ),
        RegressionStats AS (
            SELECT
                code_departement,
                type_crime,
                COUNT(*) as n_points,
                AVG(annee) as x_mean,
                AVG(taux_pour_mille) as y_mean,
                AVG(annee * taux_pour_mille) as xy_mean,
                AVG(annee * annee) as x2_mean,
                AVG(taux_pour_mille * taux_pour_mille) as y2_mean,
                STD(taux_pour_mille) as std_dev
            FROM BaseData
            GROUP BY code_departement, type_crime
            HAVING COUNT(*) > 1
        ),
        RegressionParams AS (
            SELECT 
                r.*,
                (xy_mean - x_mean * y_mean) / NULLIF(x2_mean - x_mean * x_mean, 0) as slope,
                POWER(
                    (xy_mean - x_mean * y_mean) / 
                    SQRT(
                        NULLIF(
                            (x2_mean - x_mean * x_mean) * 
                            (y2_mean - y_mean * y_mean),
                            0
                        )
                    ),
                    2
                ) as r_squared
            FROM RegressionStats r
        ),
        AllYearsCombined AS (
            SELECT 
                bd.code_departement,
                bd.type_crime,
                a.annee
            FROM BaseData bd
            CROSS JOIN Annees a
            GROUP BY bd.code_departement, bd.type_crime, a.annee
        ),
        LastHistoricalYear AS (
            SELECT MAX(annee) as derniere_annee
            FROM BaseData
        ),
        Projections AS (
            SELECT 
                a.code_departement,
                a.type_crime,
                a.annee,
                ROUND(
                    (r.y_mean - (r.slope * r.x_mean)) + (r.slope * a.annee),
                    2
                ) as projection,
                ROUND(
                    (r.y_mean - (r.slope * r.x_mean)) + (r.slope * a.annee)
                    - (1.96 * r.std_dev),
                    2
                ) as lower_bound,
                ROUND(
                    (r.y_mean - (r.slope * r.x_mean)) + (r.slope * a.annee)
                    + (1.96 * r.std_dev),
                    2
                ) as upper_bound,
                r.slope,
                r.n_points,
                COALESCE(r.r_squared, 0) as r_squared,
                CASE 
                    WHEN a.annee > (SELECT derniere_annee FROM LastHistoricalYear) THEN 'PROJECTION'
                    ELSE 'HISTORIQUE'
                END as data_type
            FROM AllYearsCombined a
            JOIN RegressionParams r ON a.code_departement = r.code_departement 
                AND a.type_crime = r.type_crime
        ),
        FinalData AS (
            SELECT 
                p.*,
                CASE 
                    WHEN p.data_type = 'HISTORIQUE' THEN 
                        (SELECT b.taux_pour_mille 
                        FROM BaseData b 
                        WHERE b.code_departement = p.code_departement 
                        AND b.type_crime = p.type_crime 
                        AND b.annee = p.annee)
                    ELSE p.projection
                END as final_value
            FROM Projections p
        )
        SELECT 
            code_departement,
            type_crime,
            annee,
            ROUND(COALESCE(final_value, projection), 2) as projection,
            lower_bound,
            upper_bound,
            slope,
            n_points,
            r_squared,
            data_type
        FROM FinalData
        ORDER BY type_crime, annee;
        """
        
        try:
            # Ajout de l'année cible dans les paramètres de la requête
            params = (
                department, crime_type, crime_type, 
                target_year,  # Nouvelle année cible
                department, crime_type, crime_type  # Répétition des paramètres pour BaseData
            )
            df = self.db.execute_query(query, params)
            recommendations = self._generate_projection_recommendations(df, target_year)
            return df, recommendations
                
        except Exception as e:
            logger.error(f"Erreur dans _projection_criminelle: {str(e)}")
            return pd.DataFrame(), f"Erreur lors de l'analyse des projections: {str(e)}"

    def _analyse_risques(self, department: str) -> Tuple[pd.DataFrame, str]:
        """Analyse les tendances et corrélations entre types de crimes"""
        query = """
        WITH BaseData AS (
            SELECT 
                s.code_departement,
                c.type_crime,
                c.annee,
                s.taux_pour_mille,
                (
                    SELECT taux_pour_mille 
                    FROM statistiques s2 
                    JOIN crimes c2 ON c2.id_crime = s2.id_crime
                    WHERE s2.code_departement = s.code_departement 
                    AND c2.type_crime = c.type_crime 
                    AND c2.annee < c.annee 
                    ORDER BY c2.annee DESC 
                    LIMIT 1
                ) as taux_precedent
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            WHERE s.code_departement = %s
        ),
        GrowthStats AS (
            SELECT 
                code_departement,
                type_crime,
                AVG(
                    CASE 
                        WHEN taux_precedent IS NULL OR taux_precedent = 0 THEN 0
                        ELSE (taux_pour_mille - taux_precedent) / taux_precedent
                    END
                ) as taux_croissance,
                MAX(CASE WHEN annee = (SELECT MAX(annee) FROM BaseData b2 WHERE b2.type_crime = BaseData.type_crime) 
                    THEN taux_pour_mille END) as derniere_valeur
            FROM BaseData
            GROUP BY code_departement, type_crime
        ),
        Projections AS (
            SELECT 
                code_departement,
                type_crime,
                taux_croissance,
                derniere_valeur,
                ROUND(
                    derniere_valeur * (1 + taux_croissance),
                    2
                ) as projection_2024,
                CASE
                    WHEN taux_croissance > 0.1 THEN 'FORTE_HAUSSE'
                    WHEN taux_croissance > 0.05 THEN 'HAUSSE_MODEREE'
                    WHEN taux_croissance < -0.1 THEN 'FORTE_BAISSE'
                    WHEN taux_croissance < -0.05 THEN 'BAISSE_MODEREE'
                    ELSE 'STABLE'
                END as tendance,
                ROUND(
                    taux_croissance * 100,
                    2
                ) as variation_projetee
            FROM GrowthStats
        ),
        CorrelationData AS (
            SELECT 
                p1.code_departement,
                p1.type_crime,
                p1.taux_croissance,
                p1.derniere_valeur,
                p1.projection_2024,
                p1.tendance,
                p1.variation_projetee,
                p2.type_crime as type_crime_2,
                ROUND(
                    (
                        SELECT 
                            (COUNT(*) * SUM(b1.taux_pour_mille * b2.taux_pour_mille) - 
                            SUM(b1.taux_pour_mille) * SUM(b2.taux_pour_mille)) /
                            SQRT(
                                (COUNT(*) * SUM(b1.taux_pour_mille * b1.taux_pour_mille) - 
                                POW(SUM(b1.taux_pour_mille), 2)) *
                                (COUNT(*) * SUM(b2.taux_pour_mille * b2.taux_pour_mille) - 
                                POW(SUM(b2.taux_pour_mille), 2))
                            )
                        FROM BaseData b1
                        JOIN BaseData b2 ON b1.annee = b2.annee 
                        WHERE b1.type_crime = p1.type_crime
                        AND b2.type_crime = p2.type_crime
                    ),
                    2
                ) as correlation
            FROM Projections p1
            CROSS JOIN Projections p2
        )
        SELECT 
            code_departement,
            type_crime,
            type_crime_2,
            taux_croissance,
            derniere_valeur,
            projection_2024,
            tendance,
            variation_projetee,
            correlation
        FROM CorrelationData
        ORDER BY type_crime, type_crime_2;
        """
        
        try:
            if not department:
                logger.error("Paramètre département manquant")
                return pd.DataFrame(), "Erreur : Département non spécifié"
                
            logger.info(f"Exécution de l'analyse des risques pour le département {department}")
            df = self.db.execute_query(query, (department,))
            
            if df.empty:
                logger.warning(f"Aucune donnée trouvée pour le département {department}")
                return df, f"Aucune donnée disponible pour le département {department}"
                
            recommendations = self._generate_risk_recommendations(df)
            
            logger.info(f"Analyse des risques terminée pour le département {department}")
            return df, recommendations
            
        except Exception as e:
            error_msg = f"Erreur dans _analyse_risques: {str(e)}"
            logger.error(error_msg)
            return pd.DataFrame(), f"Erreur lors de l'analyse des risques : {str(e)}"

    def _generate_projection_recommendations(self, df: pd.DataFrame, target_year: int) -> str:
        """Génère des recommandations basées sur les projections
        
        Args:
            df (pd.DataFrame): DataFrame contenant les données de projection
            target_year (int): Année cible de la projection
            
        Returns:
            str: Recommandations formatées
        """
        if df.empty:
            return "Aucune donnée disponible pour l'analyse"

        recommendations = [
            f"📈 Analyse des projections - Département {df['code_departement'].iloc[0]} :"
        ]

        # Analyse des tendances par type de crime
        for type_crime in df['type_crime'].unique():
            crime_data = df[df['type_crime'] == type_crime]
            last_historic = crime_data[crime_data['data_type'] == 'HISTORIQUE'].iloc[-1]
            
            # Obtention des données pour l'année cible
            target_projection = crime_data[
                (crime_data['data_type'] == 'PROJECTION') & 
                (crime_data['annee'] == target_year)
            ].iloc[0]
            
            variation = ((target_projection['projection'] - last_historic['projection']) / 
                        last_historic['projection'] * 100)
            
            recommendations.append(f"\n{type_crime}:")
            recommendations.append(
                f"- Projection 20{target_year}: {target_projection['projection']:.1f}‰ "
                f"({variation:+.1f}% vs. dernière valeur historique)"
            )
            
            # Ajout de l'intervalle de confiance
            recommendations.append(
                f"- Intervalle de confiance: [{target_projection['lower_bound']:.1f} - "
                f"{target_projection['upper_bound']:.1f}]‰"
            )
            
            # Évaluation de la fiabilité basée sur R²
            if target_projection['r_squared'] > 0.7:
                recommendations.append("- ✅ Prédiction fiable (R² > 0.7)")
            elif target_projection['r_squared'] > 0.5:
                recommendations.append("- ⚠️ Prédiction moyennement fiable (R² > 0.5)")
            else:
                recommendations.append("- ❌ Prédiction peu fiable (R² < 0.5)")
            
            # Analyse de la tendance
            if variation > 10:
                recommendations.append("- 🔴 Forte augmentation projetée")
            elif variation < -10:
                recommendations.append("- 🔵 Forte diminution projetée")
            elif abs(variation) <= 5:
                recommendations.append("- ⚪ Tendance stable")

        return "\n".join(recommendations)

    def _generate_risk_recommendations(self, df: pd.DataFrame) -> str:
        """Génère des recommandations basées sur l'analyse des risques"""
        if df.empty:
            return "Aucune donnée disponible pour l'analyse"

        recommendations = [
            f"⚠️ Analyse des risques - Département {df['code_departement'].iloc[0]} :"
        ]

        # Analyse des tendances significatives
        significant_risks = df[df['tendance'].isin(['FORTE_HAUSSE', 'FORTE_BAISSE'])]
        if not significant_risks.empty:
            recommendations.append("\nTendances significatives :")
            for _, risk in significant_risks.iterrows():
                recommendations.append(
                    f"- {risk['type_crime']}: {risk['tendance']} "
                    f"(Variation projetée: {risk['variation_projetee']:+.1f}%)"
                )

        # Analyse des corrélations fortes
        strong_correlations = df[abs(df['correlation']) > 0.7]
        if not strong_correlations.empty:
            recommendations.append("\nCorrélations significatives :")
            for _, corr in strong_correlations.iterrows():
                if corr['type_crime'] != corr['correlation']:
                    recommendations.append(
                        f"- {corr['type_crime']} et {corr['correlation']} "
                        f"évoluent de manière similaire (corr: {abs(corr['correlation']):.2f})"
                    )

        # Identification des risques prioritaires
        high_risks = df[
            (df['variation_projetee'] > 20) & 
            (df['derniere_valeur'] > df['derniere_valeur'].mean())
        ]
        if not high_risks.empty:
            recommendations.append("\nPoints d'attention prioritaires :")
            for _, risk in high_risks.iterrows():
                recommendations.append(
                    f"- {risk['type_crime']}: déjà supérieur à la moyenne et "
                    f"projection en forte hausse (+{risk['variation_projetee']:.1f}%)"
                )

        return "\n".join(recommendations)