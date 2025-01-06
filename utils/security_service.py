import pandas as pd
from typing import Tuple, List
from database.database import DatabaseConnection
from view.security_view import SecurityVisualization
import logging
import gradio as gr

logger = logging.getLogger(__name__)

class SecurityService:
    def __init__(self):
        self.db = DatabaseConnection()
        self.visualizer = SecurityVisualization()

    def process_request(
        self,
        service: str,
        department: str,
        year: int,
        department_dest: str = None,
        month: str = None,
        crime_type: str = None,
        radius: int = None
    ) -> Tuple[pd.DataFrame, str, gr.Plot, gr.Plot, gr.Plot, gr.Plot]:
        """
        Traite les demandes de service de sécurité
        Returns:
            Tuple[pd.DataFrame, str, gr.Plot, gr.Plot, gr.Plot, gr.Plot]: 
            (données, recommandations, plot1, plot2, plot3, plot4)
        """
        try:
            # Initialisation des plots vides
            empty_plots = [None] * 4
            
            # Obtenir les données et recommandations
            df, recommendations = self._get_service_data(
                service, department, year, department_dest, month, crime_type, radius
            )
            
            if df.empty:
                return (df, "Aucune donnée disponible", *empty_plots)
                
            # Générer les visualisations
            try:
                if service == "Sécurité Immobilière":
                    figures = self.visualizer.generate_security_visualizations(df)
                    # S'assurer d'avoir exactement 4 figures
                    plots = [gr.Plot(fig) if fig else None for fig in (figures + empty_plots)[:4]]
                    return (df, recommendations, *plots)
                
                elif service == "AlerteVoisinage+":
                    plots = empty_plots
                    heatmap = self.visualizer.create_risk_heatmap(df)
                    trend = self.visualizer.create_trend_analysis(df)
                    
                    if heatmap:
                        plots[0] = gr.Plot(heatmap)
                    if trend:
                        plots[1] = gr.Plot(trend)
                    
                    return (df, recommendations, *plots)
                
                return (df, recommendations, *empty_plots)
            
            except Exception as viz_error:
                logger.error(f"Erreur lors de la génération des visualisations: {viz_error}")
                return (df, recommendations, *empty_plots)
            
        except Exception as e:
            logger.error(f"Erreur dans process_request: {str(e)}")
            return (pd.DataFrame(), f"Erreur: {str(e)}", *empty_plots)

    def _get_service_data(
        self,
        service: str,
        department: str,
        year: int,
        department_dest: str = None,
        month: str = None,
        crime_type: str = None,
        radius: int = None
    ) -> Tuple[pd.DataFrame, str]:
        """Get the service specific data"""
        if service == "TransportSécurité":
            return self._transport_security(department, department_dest, year, month)
        elif service == "Sécurité Immobilière":
            return self._real_estate_security(department, year)
        elif service == "AlerteVoisinage+":
            return self._neighborhood_alert(department, year, radius)
        elif service == "BusinessSecurity":
            return self._business_security(department, year, crime_type)
        elif service == "OptimAssurance":
            return self._insurance_optimization(department, year)
        else:
            return pd.DataFrame(), "Service non reconnu"

    def _real_estate_security(self, department: str, year: int) -> Tuple[pd.DataFrame, str]:
        """Analyse les métriques de sécurité immobilière"""
        query = """
        WITH 
        -- Statistiques mensuelles nationales
        NationalStats AS (
            SELECT 
                c.type_crime,
                c.annee,
                SUM(c.nombre_faits) as total_faits_national,
                SUM(d.population) as total_population_national,
                CAST(SUM(c.nombre_faits) * 1000.0 AS DECIMAL(10,2)) / NULLIF(SUM(d.population), 0) as taux_national
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            GROUP BY c.type_crime, c.annee
        ),
        -- Statistiques mensuelles du département
        DepartmentStats AS (
            SELECT 
                d.code_departement,
                c.type_crime,
                c.annee,
                c.nombre_faits,
                d.population,
                CAST(c.nombre_faits * 1000.0 AS DECIMAL(10,2)) / NULLIF(d.population, 0) as taux_dept
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_departement = %s
            AND c.annee = %s
        ),
        -- Score de sécurité relatif
        SecurityScore AS (
            SELECT
                h.code_departement,
                h.type_crime,
                h.annee,
                h.nombre_faits,
                h.population,
                h.taux_dept,
                n.taux_national,
                -- Normalisation du score de sécurité
                CASE 
                    WHEN n.taux_national = 0 THEN 0
                    ELSE (
                        (n.taux_national - h.taux_dept) / 
                        GREATEST(n.taux_national, 0.001) * 100.0
                    )
                END as score_securite
            FROM DepartmentStats h
            JOIN NationalStats n ON h.type_crime = n.type_crime 
                AND h.annee = n.annee 
        )
        SELECT 
            *,
            CASE 
                WHEN score_securite < -20 THEN 'ÉLEVÉ'
                WHEN score_securite < 20 THEN 'MODÉRÉ'
                ELSE 'FAIBLE'
            END as niveau_risque
        FROM SecurityScore
        ORDER BY type_crime;
        """
        
        try:
            df = self.db.execute_query(query, (department, year))
            recommendations = self._generate_real_estate_recommendations(df)
            return df, recommendations
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse immobilière: {str(e)}")
            return pd.DataFrame(), "Erreur lors de l'analyse des données immobilières"
    
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

    def _transport_security(
        self, 
        dept_depart: str, 
        dept_arrivee: str, 
        annee: int, 
        mois: int
    ) -> Tuple[pd.DataFrame, str]:
        """Analyze transport security risks between two departments with validation"""
        # Validation des entrées
        if not all([dept_depart, dept_arrivee, annee, mois]):
            return pd.DataFrame(), "Données manquantes pour l'analyse"
            
        if dept_depart not in self.db.get_distinct_values("departements", "code_departement"):
            return pd.DataFrame(), f"Département de départ invalide: {dept_depart}"
            
        if dept_arrivee not in self.db.get_distinct_values("departements", "code_departement"):
            return pd.DataFrame(), f"Département d'arrivée invalide: {dept_arrivee}"
            
        query = """
        WITH TransportCrimes AS (
            -- Sélection des crimes pertinents pour les deux départements
            SELECT 
                d.code_departement,
                c.type_crime,
                c.annee,
                EXTRACT(MONTH FROM DATE(CONCAT(c.annee, '-01-01'))) as mois,
                c.nombre_faits,
                d.population,
                -- Calcul du taux pour 100 000 habitants pour normalisation
                ROUND(CAST(c.nombre_faits AS DECIMAL(10,2)) / d.population * 100000, 2) as taux_100k,
                -- Moyenne mobile sur 3 mois pour voir les tendances
                AVG(c.nombre_faits) OVER (
                    PARTITION BY d.code_departement, c.type_crime 
                    ORDER BY c.annee, EXTRACT(MONTH FROM DATE(CONCAT(c.annee, '-01-01')))
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as moyenne_mobile
            FROM crimes c
            JOIN statistiques s ON c.id_crime = s.id_crime
            JOIN departements d ON s.code_departement = d.code_departement
            WHERE d.code_departement IN (%s, %s)
            AND c.annee = %s
            AND EXTRACT(MONTH FROM DATE(CONCAT(c.annee, '-01-01'))) = %s
            AND c.type_crime IN (
                'Vols avec armes',
                'Vols violents sans arme',
                'Vols sans violence contre des personnes',
                'Vols dans les véhicules',
                'Vols de véhicules',
                'Vols d''accessoires sur véhicules',
                'Destructions et dégradations volontaires'
            )
        ),
        RiskAnalysis AS (
            SELECT 
                *,
                -- Calcul du niveau de risque basé sur le taux pour 100k habitants
                CASE 
                    WHEN taux_100k >= 50 THEN 'RISQUE ÉLEVÉ'
                    WHEN taux_100k >= 25 THEN 'RISQUE MODÉRÉ'
                    ELSE 'RISQUE FAIBLE'
                END as niveau_risque,
                -- Tendance par rapport à la moyenne mobile
                CASE
                    WHEN nombre_faits > moyenne_mobile * 1.2 THEN 'EN HAUSSE'
                    WHEN nombre_faits < moyenne_mobile * 0.8 THEN 'EN BAISSE'
                    ELSE 'STABLE'
                END as tendance
            FROM TransportCrimes
        )
        SELECT 
            *,
            -- Calcul du score de sécurité (0-100, 100 étant le plus sûr)
            GREATEST(0, LEAST(100, 100 - (taux_100k * 2))) as score_securite
        FROM RiskAnalysis
        ORDER BY score_securite ASC;
        """
        
        df = self.db.execute_query(query, (dept_depart, dept_arrivee, annee, mois))
        recommendations = self._generate_transport_route_recommendations(
            df, dept_depart, dept_arrivee, mois
        )
        return df, recommendations

    def _generate_real_estate_recommendations(self, df: pd.DataFrame) -> str:
        """Génère des recommandations pour la sécurité immobilière"""
        if df.empty:
            return "Aucune donnée disponible pour générer des recommandations"
                
        # Calcul du score moyen (100 = moyenne nationale)
        score_moyen = df['score_securite'].mean()
        
        # Détermination du niveau de risque global
        if score_moyen < -20:
            niveau_risque = "ÉLEVÉ"
        elif score_moyen < 20:
            niveau_risque = "MODÉRÉ"
        else:
            niveau_risque = "FAIBLE"
        
        recommendations = [
            f"🏘️ Analyse de sécurité immobilière :",
            f"\nNiveau de risque global: {niveau_risque}",
            f"Score de sécurité: {score_moyen:.1f} (0 = moyenne nationale)"
        ]
        
        # Analyse détaillée par type de crime
        recommendations.append("\nAnalyse détaillée :")
        current_year_data = df[df['annee'] == df['annee'].max()]
        for _, row in current_year_data.iterrows():
            score = row['score_securite']
            signe = "+" if score > 0 else ""
            recommendations.append(
                f"- {row['type_crime']}: {signe}{score:.1f} vs moyenne nationale "
                f"({row['nombre_faits']} incidents)"
            )
        
        # Recommandations spécifiques selon le niveau de risque
        recommendations.append("\nRecommandations :")
        if niveau_risque == 'ÉLEVÉ':
            recommendations.extend([
                "⚠️ Zone nécessitant des mesures de sécurité renforcées :",
                "• Installation de systèmes de sécurité avancés recommandée",
                "• Coordination avec le voisinage et les forces de l'ordre conseillée",
                "• Audit de sécurité détaillé avant acquisition",
                "• Souscription à une assurance renforcée à envisager"
            ])
        elif niveau_risque == 'MODÉRÉ':
            recommendations.extend([
                "⚠️ Vigilance recommandée :",
                "• Mesures de sécurité standards conseillées",
                "• Participation aux initiatives de voisinage vigilant",
                "• Vérification régulière des équipements de sécurité"
            ])
        else:
            recommendations.extend([
                "✅ Zone sécurisée :",
                "• Maintien des mesures de sécurité basiques",
                "• Surveillance collaborative du voisinage",
                "• Possibilité de réduction sur les assurances"
            ])
        
        # Ajout des points d'attention pour les scores très différents de la moyenne
        significant_changes = df[abs(df['score_securite']) > 30]
        if not significant_changes.empty:
            recommendations.append("\nPoints d'attention particuliers :")
            for _, change in significant_changes.iterrows():
                signe = "+" if change['score_securite'] > 0 else ""
                recommendations.append(
                    f"• {change['type_crime']}: {signe}{change['score_securite']:.1f} "
                    f"par rapport à la moyenne nationale"
                )
                    
        return "\n".join(recommendations)

    def _generate_alert_recommendations(self, df: pd.DataFrame) -> str:
        """Generate enhanced neighborhood alert recommendations"""
        if df.empty:
            return "Aucune donnée disponible pour générer des alertes"
                
        alerts = df[df['niveau_alerte'].isin(['ALERTE ROUGE', 'ALERTE ORANGE'])]
        recommendations = ["🚨 Système d'alerte de voisinage :"]
        
        # Analyse des alertes actives
        if not alerts.empty:
            recommendations.append("\nPoints d'attention critiques :")
            for _, alert in alerts.iterrows():
                z_score = alert['z_score']
                recommendations.append(
                    f"- {alert['type_crime']}: {alert['niveau_alerte']} "
                    f"(Intensité: {z_score:.1f}σ)"
                )
                
                # Recommandations selon le niveau d'alerte
                if alert['niveau_alerte'] == 'ALERTE ROUGE':
                    recommendations.extend([
                        "  • Éviter les zones isolées",
                        "  • Renforcer la vigilance collective",
                        "  • Signaler toute activité suspecte",
                        "  • Contact régulier avec les forces de l'ordre"
                    ])
                elif alert['niveau_alerte'] == 'ALERTE ORANGE':
                    recommendations.extend([
                        "  • Vigilance accrue recommandée",
                        "  • Coordination avec le voisinage",
                        "  • Vérification des dispositifs de sécurité"
                    ])
        else:
            recommendations.extend([
                "✅ Aucune alerte majeure active",
                "• Maintien de la vigilance normale",
                "• Poursuite des bonnes pratiques de sécurité"
            ])
        
        # Analyse temporelle si disponible
        if 'evolution_pourcentage' in df.columns:
            trends = df[abs(df['evolution_pourcentage']) > 15]
            if not trends.empty:
                recommendations.append("\nTendances à surveiller :")
                for _, trend in trends.iterrows():
                    recommendations.append(
                        f"- {trend['type_crime']}: {trend['evolution_pourcentage']:+.1f}% "
                        f"d'évolution"
                    )
                
        return "\n".join(recommendations)

    def _generate_business_recommendations(self, df: pd.DataFrame) -> str:
        """Generate detailed business security recommendations"""
        if df.empty:
            return "Aucune donnée disponible pour l'analyse commerciale"

        recommendations = ["💼 Analyse de sécurité commerciale :"]
        
        # Vérification de la présence des colonnes nécessaires
        required_columns = ['niveau_risque_commercial', 'type_crime', 'risque_commercial']
        if not all(col in df.columns for col in required_columns):
            return "Données insuffisantes pour l'analyse"
        
        # Filtrer les lignes avec des valeurs non nulles
        df_clean = df.dropna(subset=['niveau_risque_commercial', 'risque_commercial'])
        
        # Analyse par niveau de risque
        for risk_level in df_clean['niveau_risque_commercial'].unique():
            group = df_clean[df_clean['niveau_risque_commercial'] == risk_level]
            if group.empty:
                continue
                
            recommendations.append(f"\n{risk_level} :")
            for _, row in group.iterrows():
                risk_score = row['risque_commercial']
                if pd.notna(risk_score):  # Vérification explicite des valeurs non-NA
                    recommendations.append(
                        f"- {row['type_crime']}: {risk_score:.2f} incidents pour 10000 habitants"
                    )
                    
                    # Recommandations spécifiques selon le niveau de risque
                    if risk_level == 'CRITIQUE':
                        recommendations.append(
                            "  • Installation recommandée de système de sécurité avancé"
                            "\n  • Coordination conseillée avec les services de police"
                            "\n  • Formation du personnel aux situations à risque"
                        )
                    elif risk_level == 'ÉLEVÉ':
                        recommendations.append(
                            "  • Renforcement de la surveillance pendant les heures à risque"
                            "\n  • Mise en place de procédures de sécurité standard"
                        )

        # Analyse temporelle si disponible
        if 'evolution_pourcentage' in df.columns:
            trends = df_clean[
                df_clean['evolution_pourcentage'].notna() & 
                (abs(df_clean['evolution_pourcentage']) > 10)
            ]
            if not trends.empty:
                recommendations.append("\nTendances significatives :")
                for _, trend in trends.iterrows():
                    recommendations.append(
                        f"- {trend['type_crime']}: {trend['evolution_pourcentage']:+.1f}% "
                        f"sur la période"
                    )

        return "\n".join(recommendations)

    def _generate_insurance_recommendations(self, df: pd.DataFrame) -> str:
        """Generate detailed insurance optimization recommendations"""
        if df.empty:
            return "Aucune donnée disponible pour l'analyse assurantielle"

        # Vérification des colonnes requises
        required_columns = ['quintile_risque', 'type_crime', 'indice_relatif']
        if not all(col in df.columns for col in required_columns):
            return "Données insuffisantes pour l'analyse"

        # Nettoyage des données
        df_clean = df.dropna(subset=required_columns)
        
        recommendations = ["🔒 Analyse et recommandations assurantielles :"]
        
        # Analyse par quintile de risque
        for quintile in range(1, 6):
            quintile_data = df_clean[df_clean['quintile_risque'] == quintile]
            if quintile_data.empty:
                continue
                
            risk_level = "TRÈS ÉLEVÉ" if quintile == 5 else \
                        "ÉLEVÉ" if quintile == 4 else \
                        "MOYEN" if quintile == 3 else \
                        "FAIBLE" if quintile == 2 else "TRÈS FAIBLE"
            
            recommendations.append(f"\nNiveau de risque {risk_level} :")
            for _, row in quintile_data.iterrows():
                if pd.notna(row['indice_relatif']):
                    recommendations.append(
                        f"- {row['type_crime']}: Indice relatif {row['indice_relatif']:.1f}%"
                    )
                    
                    # Recommandations spécifiques par niveau de risque
                    if quintile >= 4:
                        recommendations.append(
                            "  • Majoration recommandée des primes"
                            "\n  • Audit de sécurité conseillé"
                            "\n  • Clauses de prévention à renforcer"
                        )
                    elif quintile == 3:
                        recommendations.append(
                            "  • Primes standards avec options de réduction"
                            "\n  • Mesures de prévention basiques conseillées"
                        )
                    else:
                        recommendations.append(
                            "  • Eligible aux réductions de prime"
                            "\n  • Offres packagées possibles"
                        )

        return "\n".join(recommendations)


    def _generate_transport_route_recommendations(
        self, 
        df: pd.DataFrame, 
        dept_depart: str, 
        dept_arrivee: str,
        mois: int
    ) -> str:
        """Generate recommendations for transport route between departments"""
        if df.empty:
            return f"Aucune donnée disponible pour l'itinéraire {dept_depart} → {dept_arrivee}"

        mois_noms = {
            1: "janvier", 2: "février", 3: "mars", 4: "avril",
            5: "mai", 6: "juin", 7: "juillet", 8: "août",
            9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre"
        }

        recommendations = [
            f"🚛 Analyse de sécurité : {dept_depart} → {dept_arrivee}",
            f"📅 Période analysée : {mois_noms[mois]}"
        ]

        # 1. Analyse des départements
        for dept in [dept_depart, dept_arrivee]:
            dept_data = df[df['code_departement'] == dept]
            if not dept_data.empty:
                score_moyen = dept_data['score_securite'].mean()
                recommendations.append(
                    f"\n📍 Département {dept} "
                    f"(Score de sécurité: {score_moyen:.0f}/100) :"
                )

                # Points d'attention spécifiques avec tendances
                risques_eleves = dept_data[
                    (dept_data['niveau_risque'] == 'RISQUE ÉLEVÉ') |
                    (dept_data['tendance'] == 'EN HAUSSE')
                ]
                for _, risque in risques_eleves.iterrows():
                    tendance_icon = "📈" if risque['tendance'] == 'EN HAUSSE' else "📊"
                    recommendations.append(
                        f"- {tendance_icon} {risque['type_crime']}: "
                        f"{risque['taux_100k']:.1f} incidents/100k hab. "
                        f"({risque['tendance'].lower()})"
                    )

        # 2. Comparaison des risques entre départements
        recommendations.append("\n🔄 Analyse comparative :")
        for type_crime in df['type_crime'].unique():
            crime_data = df[df['type_crime'] == type_crime]
            if len(crime_data) == 2:  # Si on a des données pour les deux départements
                depart_rate = crime_data[crime_data['code_departement'] == dept_depart]['taux_100k'].iloc[0]
                arrivee_rate = crime_data[crime_data['code_departement'] == dept_arrivee]['taux_100k'].iloc[0]
                diff_rate = abs(depart_rate - arrivee_rate)
                
                if diff_rate > 10:  # Différence significative
                    higher_dept = dept_arrivee if arrivee_rate > depart_rate else dept_depart
                    recommendations.append(
                        f"- {type_crime}: {diff_rate:.1f} incidents/100k hab. de plus dans le dept {higher_dept}"
                    )

        # 3. Points de vigilance pour l'itinéraire
        risques_majeurs = df[
            (df['niveau_risque'] == 'RISQUE ÉLEVÉ') & 
            (df['tendance'] == 'EN HAUSSE')
        ]
        if not risques_majeurs.empty:
            recommendations.append("\n⚠️ Points de vigilance majeurs :")
            for _, risque in risques_majeurs.iterrows():
                recommendations.append(
                    f"- Dept {risque['code_departement']}: {risque['type_crime']} "
                    f"({risque['taux_100k']:.1f} incidents/100k hab.)"
                )

        return "\n".join(recommendations)