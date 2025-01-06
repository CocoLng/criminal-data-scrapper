import pandas as pd
from typing import Tuple, Dict, Any
from database.database import DatabaseConnection
import logging
import math

logger = logging.getLogger(__name__)

class PredictiveService:
    def __init__(self):
        self.db = DatabaseConnection()

    def process_request(
        self,
        service: str,
        crime_type: str = None,
        department: str = None,
        horizon: str = None
    ) -> Tuple[pd.DataFrame, str]:
        """Process predictive analysis requests"""
        try:
            if service == "Prévision Saisonnière":
                return self._seasonal_prediction(crime_type, department)
            elif service == "PolicePrédictive":
                return self._predictive_policing(department, horizon)
            else:
                return pd.DataFrame(), "Service non reconnu"
        except Exception as e:
            logger.error(f"Error in PredictiveService: {e}")
            return pd.DataFrame(), f"Erreur: {str(e)}"

    def _seasonal_prediction(self, crime_type: str, department: str) -> Tuple[pd.DataFrame, str]:
        """Generate seasonal predictions for crime trends"""
        query = """
        WITH SeasonalData AS (
            SELECT 
                d.code_departement,
                c.type_crime,
                c.annee,
                CASE 
                    WHEN EXTRACT(MONTH FROM DATE(CONCAT(c.annee, '-01-01'))) IN (12,1,2) THEN 'HIVER'
                    WHEN EXTRACT(MONTH FROM DATE(CONCAT(c.annee, '-01-01'))) IN (3,4,5) THEN 'PRINTEMPS'
                    WHEN EXTRACT(MONTH FROM DATE(CONCAT(c.annee, '-01-01'))) IN (6,7,8) THEN 'ETE'
                    ELSE 'AUTOMNE'
                END as saison,
                SUM(c.nombre_faits) as total_faits,
                AVG(s.taux_pour_mille) as taux_moyen
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE c.type_crime = %s 
            AND d.code_departement = %s
            GROUP BY d.code_departement, c.type_crime, c.annee, saison
        ),
        SeasonalStats AS (
            SELECT 
                code_departement,
                type_crime,
                saison,
                AVG(total_faits) as moyenne_faits,
                STDDEV(total_faits) as ecart_type,
                (
                    (COUNT(*) * SUM(annee * total_faits) - SUM(annee) * SUM(total_faits)) /
                    (COUNT(*) * SUM(annee * annee) - SUM(annee) * SUM(annee))
                ) as tendance,
                (
                    POW(
                        (COUNT(*) * SUM(annee * total_faits) - SUM(annee) * SUM(total_faits)) /
                        SQRT(
                            (COUNT(*) * SUM(annee * annee) - POW(SUM(annee), 2)) *
                            (COUNT(*) * SUM(total_faits * total_faits) - POW(SUM(total_faits), 2))
                        )
                    , 2)
                ) as r2,
                MAX(annee) as derniere_annee
            FROM SeasonalData
            GROUP BY code_departement, type_crime, saison
        ),
        PredictionBase AS (
            SELECT 
                *,
                moyenne_faits + (tendance * 1) as prediction_prochaine_saison,
                CASE 
                    WHEN ABS(tendance / NULLIF(moyenne_faits, 0)) > 0.1 THEN
                        CASE 
                            WHEN tendance > 0 THEN 'HAUSSE_SIGNIFICATIVE'
                            ELSE 'BAISSE_SIGNIFICATIVE'
                        END
                    ELSE 'STABLE'
                END as tendance_interpretation,
                ecart_type / NULLIF(moyenne_faits, 0) as coefficient_variation
            FROM SeasonalStats
        )
        SELECT 
            *,
            CASE 
                WHEN coefficient_variation > 0.5 THEN 'FORTE'
                WHEN coefficient_variation > 0.25 THEN 'MOYENNE'
                ELSE 'FAIBLE'
            END as volatilite,
            prediction_prochaine_saison - 2 * ecart_type as borne_inf_prediction,
            prediction_prochaine_saison + 2 * ecart_type as borne_sup_prediction
        FROM PredictionBase
        ORDER BY saison;
        """
        
        df = self.db.execute_query(query, (crime_type, department))
        recommendations = self._generate_seasonal_recommendations(df)
        return df, recommendations

    def _predictive_policing(self, department: str, horizon: str) -> Tuple[pd.DataFrame, str]:
        """Generate predictive policing analysis with fixed window functions"""
        horizon_months = self._parse_horizon(horizon)
        
        query = """
        WITH HistoricalPatterns AS (
            SELECT 
                d.code_departement,
                c.type_crime,
                c.annee,
                EXTRACT(MONTH FROM DATE(CONCAT(c.annee, '-01-01'))) as mois,
                c.nombre_faits,
                s.taux_pour_mille,
                AVG(c.nombre_faits) OVER (
                    PARTITION BY d.code_departement, c.type_crime 
                    ORDER BY c.annee, EXTRACT(MONTH FROM DATE(CONCAT(c.annee, '-01-01')))
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ) as moyenne_mobile
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_departement = %s
        ),
        TimeSeriesData AS (
            SELECT 
                code_departement,
                type_crime,
                annee,
                mois,
                nombre_faits,
                moyenne_mobile,
                @row_num := @row_num + 1 AS x_value
            FROM HistoricalPatterns
            CROSS JOIN (SELECT @row_num := 0) AS vars
            ORDER BY annee, mois
        ),
        MovingAverages AS (
            SELECT 
                *,
                AVG(nombre_faits) OVER (
                    PARTITION BY code_departement, type_crime
                    ORDER BY annee, mois
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as y_avg,
                AVG(x_value) OVER (
                    PARTITION BY code_departement, type_crime
                    ORDER BY annee, mois
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as x_avg
            FROM TimeSeriesData
        ),
        TrendCalculation AS (
            SELECT 
                *,
                SUM((x_value - x_avg) * (nombre_faits - y_avg)) OVER (
                    PARTITION BY code_departement, type_crime
                    ORDER BY annee, mois
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) / NULLIF(
                    SUM(POW(x_value - x_avg, 2)) OVER (
                        PARTITION BY code_departement, type_crime
                        ORDER BY annee, mois
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ),
                    0
                ) as tendance_locale
            FROM MovingAverages
        ),
        SeasonalFactors AS (
            SELECT 
                *,
                nombre_faits - AVG(nombre_faits) OVER (
                    PARTITION BY code_departement, type_crime, annee
                ) as composante_saisonniere
            FROM TrendCalculation
        )
        SELECT 
            code_departement,
            type_crime,
            annee,
            mois,
            nombre_faits,
            moyenne_mobile,
            tendance_locale,
            composante_saisonniere,
            moyenne_mobile + (tendance_locale * %s) + 
            COALESCE(
                AVG(composante_saisonniere) OVER (
                    PARTITION BY code_departement, type_crime, mois
                ),
                0
            ) as prediction_base,
            STDDEV(nombre_faits) OVER (
                PARTITION BY code_departement, type_crime
                ORDER BY annee, mois
                ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
            ) * 1.96 as intervalle_confiance,
            CASE 
                WHEN tendance_locale > 0 AND ABS(tendance_locale) > 0.1 THEN 'HAUSSE'
                WHEN tendance_locale < 0 AND ABS(tendance_locale) > 0.1 THEN 'BAISSE'
                ELSE 'STABLE'
            END as tendance_prevue,
            CASE 
                WHEN moyenne_mobile > AVG(moyenne_mobile) OVER (PARTITION BY code_departement)
                THEN 'VIGILANCE ACCRUE'
                ELSE 'NORMAL'
            END as niveau_vigilance
        FROM SeasonalFactors
        WHERE annee = (SELECT MAX(annee) FROM HistoricalPatterns)
        ORDER BY prediction_base DESC;
        """
        
        df = self.db.execute_query(query, (department, horizon_months))
        recommendations = self._generate_policing_recommendations(df)
        return df, recommendations

    def _parse_horizon(self, horizon: str) -> int:
        """Convert horizon string to number of months"""
        if horizon == "1 mois":
            return 1
        elif horizon == "3 mois":
            return 3
        elif horizon == "6 mois":
            return 6
        elif horizon == "1 an":
            return 12
        return 3 # Défaut: 3 mois

    def _generate_seasonal_recommendations(self, df: pd.DataFrame) -> str:
        """Generate recommendations based on seasonal predictions"""
        if df.empty:
            return "Aucune donnée disponible pour les prédictions saisonnières"

        recommendations = ["🔮 Prévisions saisonnières :"]
        
        # Analyse par saison
        for _, row in df.iterrows():
            recommendations.append(f"\n{row['saison']}:")
            recommendations.append(
                f"- Prévision: {row['prediction_prochaine_saison']:.0f} faits "
                f"({row['tendance_interpretation']})"
            )
            recommendations.append(
                f"- Intervalle de confiance: [{row['borne_inf_prediction']:.0f} - "
                f"{row['borne_sup_prediction']:.0f}]"
            )
            recommendations.append(f"- Volatilité: {row['volatilite']}")

        # Recommandations générales
        recommendations.append("\nRecommandations :")
        for _, row in df.iterrows():
            if row['tendance_interpretation'] == 'HAUSSE_SIGNIFICATIVE':
                recommendations.append(
                    f"⚠️ Renforcement conseillé pour {row['saison'].lower()}"
                )

        return "\n".join(recommendations)

    def _generate_policing_recommendations(self, df: pd.DataFrame) -> str:
        """Generate recommendations based on predictive policing analysis"""
        if df.empty:
            return "Aucune donnée disponible pour les prédictions"

        recommendations = ["👮 Recommandations opérationnelles :"]
        
        # Analyse des zones prioritaires
        high_priority = df[df['niveau_vigilance'] == 'VIGILANCE ACCRUE']
        if not high_priority.empty:
            recommendations.append("\nZones nécessitant une vigilance accrue :")
            for _, zone in high_priority.iterrows():
                recommendations.append(
                    f"- {zone['type_crime']}: {zone['prediction_base']:.0f} faits prévus "
                    f"(+{((zone['prediction_base']/zone['moyenne_mobile'] - 1) * 100):.1f}%)"
                )

        # Tendances générales
        recommendations.append("\nTendances identifiées :")
        for tendency in df['tendance_prevue'].unique():
            types = df[df['tendance_prevue'] == tendency]['type_crime'].tolist()
            if types:
                recommendations.append(f"- {tendency}: {', '.join(types)}")

        return "\n".join(recommendations)