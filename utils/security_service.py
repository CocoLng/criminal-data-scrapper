import pandas as pd
from typing import Tuple
from database.database import DatabaseConnection
import logging

logger = logging.getLogger(__name__)

class SecurityService:
    def __init__(self):
        self.db = DatabaseConnection()

    def process_request(
        self,
        service: str,
        department: str,
        year: int,
        crime_type: str = None,
        radius: int = None
    ) -> Tuple[pd.DataFrame, str]:
        """Process security service requests"""
        try:
            if service == "Sécurité Immobilière":
                return self._real_estate_security(department, year)
            elif service == "AlerteVoisinage+":
                return self._neighborhood_alert(department, year, radius)
            elif service == "BusinessSecurity":
                return self._business_security(department, year, crime_type)
            elif service == "OptimAssurance":
                return self._insurance_optimization(department, year)
            elif service == "TransportSécurité":
                return self._transport_security(department, year)
            else:
                return pd.DataFrame(), "Service non reconnu"
        except Exception as e:
            logger.error(f"Error in SecurityService: {e}")
            return pd.DataFrame(), f"Erreur: {str(e)}"

    def _real_estate_security(self, department: str, year: int) -> Tuple[pd.DataFrame, str]:
        """Analyze real estate security metrics"""
        query = """
        WITH CrimeStats AS (
            SELECT 
                d.code_departement,
                c.type_crime,
                c.nombre_faits,
                d.logements,
                d.population,
                CAST(c.nombre_faits AS DECIMAL(10,4)) / NULLIF(d.logements, 0) as ratio_crime_logement,
                CAST(c.nombre_faits AS DECIMAL(10,4)) / NULLIF(d.population, 0) * 1000 as taux_population
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_departement = %s AND c.annee = %s
        ),
        SecurityScore AS (
            SELECT
                code_departement,
                ROUND(AVG(ratio_crime_logement), 4) as score_logement,
                ROUND(AVG(taux_population), 2) as score_population,
                COUNT(DISTINCT type_crime) as nb_types_crimes,
                SUM(nombre_faits) as total_faits
            FROM CrimeStats
            GROUP BY code_departement
        )
        SELECT 
            cs.*,
            ss.score_logement,
            ss.score_population,
            CASE 
                WHEN ss.score_logement > 0.1 THEN 'ÉLEVÉ'
                WHEN ss.score_logement > 0.05 THEN 'MODÉRÉ'
                ELSE 'FAIBLE'
            END as niveau_risque
        FROM CrimeStats cs
        JOIN SecurityScore ss USING (code_departement)
        ORDER BY cs.ratio_crime_logement DESC;
        """
        
        df = self.db.execute_query(query, (department, year))
        recommendations = self._generate_real_estate_recommendations(df)
        return df, recommendations

    def _neighborhood_alert(self, department: str, year: int, radius: int) -> Tuple[pd.DataFrame, str]:
        """Generate neighborhood alerts and risk analysis"""
        query = """
        WITH TemporalTrends AS (
            SELECT 
                d.code_departement,
                c.type_crime,
                c.annee,
                c.nombre_faits,
                AVG(c.nombre_faits) OVER (
                    PARTITION BY d.code_departement, c.type_crime
                    ORDER BY c.annee
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as moyenne_mobile,
                STDDEV(c.nombre_faits) OVER (
                    PARTITION BY d.code_departement, c.type_crime
                    ORDER BY c.annee
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as ecart_type
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_departement = %s
        ),
        RiskAssessment AS (
            SELECT 
                code_departement,
                type_crime,
                annee,
                nombre_faits,
                moyenne_mobile,
                (nombre_faits - moyenne_mobile) / NULLIF(ecart_type, 0) as z_score
            FROM TemporalTrends
            WHERE annee = %s
        )
        SELECT 
            *,
            CASE 
                WHEN z_score > 2 THEN 'ALERTE ROUGE'
                WHEN z_score > 1 THEN 'ALERTE ORANGE'
                WHEN z_score > 0 THEN 'VIGILANCE'
                ELSE 'NORMAL'
            END as niveau_alerte
        FROM RiskAssessment
        ORDER BY z_score DESC;
        """
        
        df = self.db.execute_query(query, (department, year))
        recommendations = self._generate_alert_recommendations(df)
        return df, recommendations

    def _business_security(self, department: str, year: int, crime_type: str) -> Tuple[pd.DataFrame, str]:
        """Analyze business security risks"""
        query = """
        WITH BusinessRisks AS (
            SELECT 
                d.code_departement,
                c.type_crime,
                c.nombre_faits,
                s.taux_pour_mille,
                d.population,
                d.logements,
                CAST(c.nombre_faits AS DECIMAL(10,4)) / NULLIF(d.population, 0) * 10000 as risque_commercial
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_departement = %s 
            AND c.annee = %s
            AND (%s IS NULL OR c.type_crime = %s)
        )
        SELECT 
            *,
            CASE 
                WHEN risque_commercial > (SELECT AVG(risque_commercial) * 2 FROM BusinessRisks) THEN 'CRITIQUE'
                WHEN risque_commercial > (SELECT AVG(risque_commercial) FROM BusinessRisks) THEN 'ÉLEVÉ'
                ELSE 'MODÉRÉ'
            END as niveau_risque_commercial
        FROM BusinessRisks
        ORDER BY risque_commercial DESC;
        """
        
        df = self.db.execute_query(query, (department, year, crime_type, crime_type))
        recommendations = self._generate_business_recommendations(df)
        return df, recommendations

    def _insurance_optimization(self, department: str, year: int) -> Tuple[pd.DataFrame, str]:
        """Calculate insurance risk scores"""
        query = """
        WITH RiskMetrics AS (
            SELECT 
                d.code_departement,
                c.type_crime,
                c.nombre_faits,
                d.population,
                d.logements,
                CAST(c.nombre_faits AS DECIMAL(10,4)) / NULLIF(d.logements, 0) as risque_logement,
                CAST(c.nombre_faits AS DECIMAL(10,4)) / NULLIF(d.population, 0) * 1000 as risque_population
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_departement = %s AND c.annee = %s
        ),
        InsuranceScore AS (
            SELECT
                code_departement,
                type_crime,
                risque_logement,
                risque_population,
                (risque_logement * 0.6 + risque_population * 0.4) as score_assurance
            FROM RiskMetrics
        )
        SELECT 
            *,
            NTILE(5) OVER (ORDER BY score_assurance) as quintile_risque,
            ROUND(score_assurance / (SELECT AVG(score_assurance) FROM InsuranceScore) * 100, 2) as indice_relatif
        FROM InsuranceScore
        ORDER BY score_assurance DESC;
        """
        
        df = self.db.execute_query(query, (department, year))
        recommendations = self._generate_insurance_recommendations(df)
        return df, recommendations

    def _transport_security(self, department: str, year: int) -> Tuple[pd.DataFrame, str]:
        """Analyze transport security risks"""
        query = """
        WITH TransportRisks AS (
            SELECT 
                d.code_departement,
                c.type_crime,
                c.annee,
                c.nombre_faits,
                d.population,
                LAG(c.nombre_faits) OVER (PARTITION BY d.code_departement, c.type_crime ORDER BY c.annee) as faits_precedents,
                EXTRACT(MONTH FROM DATE(CONCAT(c.annee, '-01-01'))) as mois
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_departement = %s AND c.annee = %s
        ),
        RiskZones AS (
            SELECT 
                *,
                CASE 
                    WHEN mois IN (6,7,8) THEN 'ÉTÉ'
                    WHEN mois IN (9,10,11) THEN 'AUTOMNE'
                    WHEN mois IN (12,1,2) THEN 'HIVER'
                    ELSE 'PRINTEMPS'
                END as saison,
                CAST((nombre_faits - faits_precedents) AS DECIMAL(10,4)) / NULLIF(faits_precedents, 0) * 100 as evolution_pourcentage
            FROM TransportRisks
        )
        SELECT 
            *,
            CASE 
                WHEN evolution_pourcentage > 20 THEN 'ZONE ROUGE'
                WHEN evolution_pourcentage > 10 THEN 'ZONE ORANGE'
                WHEN evolution_pourcentage > 0 THEN 'ZONE JAUNE'
                ELSE 'ZONE VERTE'
            END as classification_risque
        FROM RiskZones
        ORDER BY evolution_pourcentage DESC;
        """
        
        df = self.db.execute_query(query, (department, year))
        recommendations = self._generate_transport_recommendations(df)
        return df, recommendations

    def _generate_real_estate_recommendations(self, df: pd.DataFrame) -> str:
        """Generate recommendations based on real estate security analysis"""
        if df.empty:
            return "Aucune donnée disponible pour générer des recommandations"
            
        avg_score = df['score_logement'].mean()
        risk_level = df['niveau_risque'].iloc[0]
        
        recommendations = [
            f"Niveau de risque global: {risk_level}",
            f"Score moyen de sécurité: {avg_score:.4f}"
        ]
        
        if risk_level == 'ÉLEVÉ':
            recommendations.append("⚠️ Zone nécessitant des mesures de sécurité renforcées")
        elif risk_level == 'MODÉRÉ':
            recommendations.append("⚠️ Vigilance recommandée, mesures de sécurité standards conseillées")
        else:
            recommendations.append("✅ Zone présentant un bon niveau de sécurité")
            
        return "\n".join(recommendations)

    def _generate_alert_recommendations(self, df: pd.DataFrame) -> str:
        """Generate recommendations for neighborhood alerts"""
        if df.empty:
            return "Aucune donnée disponible pour générer des alertes"
            
        alerts = df[df['niveau_alerte'].isin(['ALERTE ROUGE', 'ALERTE ORANGE'])]
        recommendations = []
        
        if not alerts.empty:
            recommendations.append("⚠️ Points d'attention :")
            for _, alert in alerts.iterrows():
                recommendations.append(
                    f"- {alert['type_crime']}: {alert['niveau_alerte']} "
                    f"(+{alert['z_score']:.1f}σ par rapport à la normale)"
                )
        else:
            recommendations.append("✅ Aucune alerte majeure détectée")
            
        return "\n".join(recommendations)

    def _generate_business_recommendations(self, df: pd.DataFrame) -> str:
        """Generate business security recommendations"""
        pass  # Implementation similaire aux autres méthodes de recommandation

    def _generate_insurance_recommendations(self, df: pd.DataFrame) -> str:
        """Generate insurance optimization recommendations"""
        pass  # Implementation similaire aux autres méthodes de recommandation

    def _generate_transport_recommendations(self, df: pd.DataFrame) -> str:
        """Generate transport security recommendations"""
        pass  # Implementation similaire aux autres méthodes de recommandation