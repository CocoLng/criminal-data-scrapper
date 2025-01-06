import pandas as pd
from typing import Tuple, Dict, Any
from database.database import DatabaseConnection
import logging

logger = logging.getLogger(__name__)

class TerritorialService:
    def __init__(self):
        self.db = DatabaseConnection()

    def process_request(
        self,
        service: str,
        department: str,
        comparison_type: str
    ) -> Tuple[pd.DataFrame, str]:
        """Process territorial analysis requests"""
        try:
            if service == "Diagnostic Territorial":
                return self._territorial_diagnostic(department, comparison_type)
            elif service == "Impact Événementiel":
                return self._event_impact(department)
            elif service == "UrbanSafe":
                return self._urban_safety(department)
            else:
                return pd.DataFrame(), "Service non reconnu"
        except Exception as e:
            logger.error(f"Error in TerritorialService: {e}")
            return pd.DataFrame(), f"Erreur: {str(e)}"

    def _territorial_diagnostic(self, department: str, comparison_type: str) -> Tuple[pd.DataFrame, str]:
        """Analyze and compare territories based on various metrics"""
        comparison_clause = self._get_comparison_clause(comparison_type)
        
        query = f"""
        WITH BaseMetrics AS (
            SELECT 
                d.code_departement,
                d.code_region,
                d.population,
                d.logements,
                CAST(d.population AS DECIMAL(10,4)) / NULLIF(d.logements, 0) as densite_logement
            FROM departements d
        ),
        ComparableDepts AS (
            SELECT 
                d1.code_departement as dept_ref,
                d2.code_departement as dept_comp,
                ABS(d1.population - d2.population) / GREATEST(d1.population, d2.population) as diff_population,
                ABS(d1.densite_logement - d2.densite_logement) as diff_densite
            FROM BaseMetrics d1
            CROSS JOIN BaseMetrics d2
            WHERE d1.code_departement = %s
            AND d1.code_departement != d2.code_departement
            {comparison_clause}
        ),
        CrimeComparison AS (
            SELECT 
                cd.dept_ref,
                cd.dept_comp,
                c.type_crime,
                c.annee,
                SUM(CASE WHEN s.code_departement = cd.dept_ref THEN c.nombre_faits END) as faits_ref,
                SUM(CASE WHEN s.code_departement = cd.dept_comp THEN c.nombre_faits END) as faits_comp,
                AVG(CASE WHEN s.code_departement = cd.dept_comp THEN s.taux_pour_mille END) as taux_comp
            FROM ComparableDepts cd
            JOIN statistiques s ON s.code_departement IN (cd.dept_ref, cd.dept_comp)
            JOIN crimes c ON s.id_crime = c.id_crime
            GROUP BY cd.dept_ref, cd.dept_comp, c.type_crime, c.annee
        )
        SELECT 
            cc.*,
            bm_ref.population as population_ref,
            bm_ref.logements as logements_ref,
            bm_comp.population as population_comp,
            bm_comp.logements as logements_comp,
            ROUND(((CAST(faits_comp AS DECIMAL(10,4)) - faits_ref) / NULLIF(faits_ref, 0) * 100), 2) as diff_pourcentage
        FROM CrimeComparison cc
        JOIN BaseMetrics bm_ref ON cc.dept_ref = bm_ref.code_departement
        JOIN BaseMetrics bm_comp ON cc.dept_comp = bm_comp.code_departement
        ORDER BY ABS(diff_pourcentage) DESC;
        """
        
        df = self.db.execute_query(query, (department,))
        recommendations = self._generate_territorial_recommendations(df)
        return df, recommendations

    def _event_impact(self, department: str) -> Tuple[pd.DataFrame, str]:
        """Analyze the impact of events on crime rates"""
        query = """
        WITH MonthlyStats AS (
            SELECT 
                d.code_departement,
                c.type_crime,
                c.annee,
                EXTRACT(MONTH FROM DATE(CONCAT(c.annee, '-01-01'))) as mois,
                c.nombre_faits,
                AVG(c.nombre_faits) OVER (
                    PARTITION BY d.code_departement, c.type_crime, 
                    EXTRACT(MONTH FROM DATE(CONCAT(c.annee, '-01-01')))
                    ORDER BY c.annee
                    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
                ) as moyenne_historique,
                STDDEV(c.nombre_faits) OVER (
                    PARTITION BY d.code_departement, c.type_crime,
                    EXTRACT(MONTH FROM DATE(CONCAT(c.annee, '-01-01')))
                    ORDER BY c.annee
                    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
                ) as ecart_type_historique
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_departement = %s
        ),
        Anomalies AS (
            SELECT 
                *,
                (nombre_faits - moyenne_historique) / NULLIF(ecart_type_historique, 0) as z_score,
                CASE 
                    WHEN mois IN (7,8) THEN 'PÉRIODE ESTIVALE'
                    WHEN mois IN (12) THEN 'FÊTES DE FIN D''ANNÉE'
                    WHEN mois IN (1) THEN 'SOLDES D''HIVER'
                    WHEN mois IN (6) THEN 'SOLDES D''ÉTÉ'
                    ELSE 'PÉRIODE NORMALE'
                END as periode_evenement
            FROM MonthlyStats
        )
        SELECT 
            *,
            CASE 
                WHEN z_score > 2 THEN 'IMPACT MAJEUR'
                WHEN z_score > 1 THEN 'IMPACT SIGNIFICATIF'
                WHEN z_score < -1 THEN 'IMPACT POSITIF'
                ELSE 'IMPACT NORMAL'
            END as niveau_impact
        FROM Anomalies
        WHERE ABS(z_score) > 1
        ORDER BY ABS(z_score) DESC;
        """
        
        df = self.db.execute_query(query, (department,))
        recommendations = self._generate_event_recommendations(df)
        return df, recommendations

    def _urban_safety(self, department: str) -> Tuple[pd.DataFrame, str]:
        """Analyze urban safety metrics and provide planning recommendations"""
        query = """
        WITH UrbanMetrics AS (
            SELECT 
                d.code_departement,
                d.population,
                d.logements,
                c.type_crime,
                c.nombre_faits,
                CAST(d.population AS DECIMAL(10,4)) / NULLIF(d.logements, 0) as densite_logement,
                CAST(c.nombre_faits AS DECIMAL(10,4)) / NULLIF(d.logements, 0) as ratio_crime_logement,
                CAST(c.nombre_faits AS DECIMAL(10,4)) / NULLIF(d.population, 0) * 1000 as taux_crime_population
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_departement = %s
        ),
        SafetyScore AS (
            SELECT 
                code_departement,
                densite_logement,
                ROUND(AVG(ratio_crime_logement), 4) as score_crime_logement,
                ROUND(AVG(taux_crime_population), 2) as score_crime_population,
                (
                    (COUNT(*) * SUM(ratio_crime_logement * densite_logement) - SUM(ratio_crime_logement) * SUM(densite_logement)) /
                    SQRT(
                        (COUNT(*) * SUM(ratio_crime_logement * ratio_crime_logement) - SUM(ratio_crime_logement) * SUM(ratio_crime_logement)) *
                        (COUNT(*) * SUM(densite_logement * densite_logement) - SUM(densite_logement) * SUM(densite_logement))
                    )
                ) as correlation_densite_crime
            FROM UrbanMetrics
            GROUP BY code_departement, densite_logement
        )
        SELECT 
            um.*,
            ss.score_crime_logement,
            ss.score_crime_population,
            ss.correlation_densite_crime,
            CASE 
                WHEN um.ratio_crime_logement > ss.score_crime_logement * 1.5 THEN 'ZONE CRITIQUE'
                WHEN um.ratio_crime_logement > ss.score_crime_logement * 1.2 THEN 'ZONE À RISQUE'
                WHEN um.ratio_crime_logement < ss.score_crime_logement * 0.8 THEN 'ZONE SÉCURISÉE'
                ELSE 'ZONE NORMALE'
            END as classification_zone
        FROM UrbanMetrics um
        JOIN SafetyScore ss USING (code_departement)
        ORDER BY um.ratio_crime_logement DESC;
        """
        
        df = self.db.execute_query(query, (department,))
        recommendations = self._generate_urban_recommendations(df)
        return df, recommendations

    def _get_comparison_clause(self, comparison_type: str) -> str:
        """Generate SQL clause for territory comparison based on type"""
        if comparison_type == "Population similaire":
            return "AND ABS(d1.population - d2.population) / GREATEST(d1.population, d2.population) < 0.2"
        elif comparison_type == "Même région":
            return "AND d1.code_region = d2.code_region"
        elif comparison_type == "Départements limitrophes":
            # Cette partie nécessiterait une table de départements limitrophes
            return "AND 1=1"  # À adapter selon les données disponibles
        return ""

    def _generate_territorial_recommendations(self, df: pd.DataFrame) -> str:
        """Generate recommendations based on territorial analysis"""
        if df.empty:
            return "Aucune donnée disponible pour l'analyse territoriale"

        recommendations = ["📊 Analyse territoriale comparative :"]
        
        # Analyse des écarts significatifs
        significant_diff = df[abs(df['diff_pourcentage']) > 20]
        if not significant_diff.empty:
            recommendations.append("\nÉcarts significatifs détectés :")
            for _, row in significant_diff.iterrows():
                recommendations.append(
                    f"- {row['type_crime']}: {row['diff_pourcentage']:+.1f}% "
                    f"par rapport au département {row['dept_comp']}"
                )

        # Tendances générales
        avg_diff = df['diff_pourcentage'].mean()
        recommendations.append(f"\nTendance générale: {avg_diff:+.1f}% par rapport aux territoires comparables")

        return "\n".join(recommendations)

    def _generate_event_recommendations(self, df: pd.DataFrame) -> str:
        """Generate recommendations based on event impact analysis"""
        if df.empty:
            return "Aucun impact événementiel significatif détecté"

        recommendations = ["🎉 Analyse de l'impact événementiel :"]
        
        for periode in df['periode_evenement'].unique():
            period_data = df[df['periode_evenement'] == periode]
            if not period_data.empty:
                recommendations.append(f"\n{periode}:")
                for _, row in period_data.iterrows():
                    recommendations.append(
                        f"- {row['type_crime']}: {row['niveau_impact']} "
                        f"(z-score: {row['z_score']:.2f})"
                    )

        return "\n".join(recommendations)

    def _generate_urban_recommendations(self, df: pd.DataFrame) -> str:
        """Generate recommendations based on urban safety analysis"""
        if df.empty:
            return "Aucune donnée disponible pour l'analyse urbaine"

        recommendations = ["🏘️ Analyse de la sécurité urbaine :"]
        
        # Analyse des zones critiques
        critical_zones = df[df['classification_zone'] == 'ZONE CRITIQUE']
        if not critical_zones.empty:
            recommendations.append("\nZones nécessitant une attention immédiate :")
            for _, zone in critical_zones.iterrows():
                recommendations.append(
                    f"- {zone['type_crime']}: ratio de {zone['ratio_crime_logement']:.4f} "
                    f"crimes par logement"
                )

        # Corrélation densité-crime
        if 'correlation_densite_crime' in df.columns:
            corr = df['correlation_densite_crime'].iloc[0]
            if abs(corr) > 0.5:
                recommendations.append(
                    f"\nCorrélation significative entre densité et criminalité: {corr:.2f}"
                )

        return "\n".join(recommendations)