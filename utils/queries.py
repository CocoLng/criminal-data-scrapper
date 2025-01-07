from typing import Dict


class QueryBuilder:
    @staticmethod
    def get_predefined_queries() -> Dict[str, Dict]:
        """Returns a dictionary of predefined queries with their descriptions and required parameters"""
        return {
            "Analyse par département": {
                "query": """
                    SELECT 
                        d.code_departement,
                        d.code_region,
                        c.type_crime,
                        c.nombre_faits,
                        s.taux_pour_mille,
                        d.population,
                        c.annee
                    FROM statistiques s
                    JOIN crimes c ON s.id_crime = c.id_crime
                    JOIN departements d ON s.code_departement = d.code_departement
                    WHERE 
                        d.code_departement = %s
                        AND c.annee = %s
                    ORDER BY c.type_crime
                """,
                "params": ["code_departement", "annee"],
            },
            "Evolution d'un type de crime": {
                "query": """
                    SELECT 
                        c.annee,
                        SUM(c.nombre_faits) as total_faits,
                        AVG(s.taux_pour_mille) as taux_moyen
                    FROM crimes c
                    LEFT JOIN statistiques s ON c.id_crime = s.id_crime
                    WHERE c.type_crime = %s
                    GROUP BY c.annee
                    ORDER BY c.annee
                """,
                "params": ["type_crime"],
            },
            "Comparaison régionale": {
                "query": """
                    SELECT 
                        d.code_region,
                        c.type_crime,
                        c.annee,
                        SUM(c.nombre_faits) as total_faits,
                        AVG(s.taux_pour_mille) as taux_moyen,
                        SUM(d.population) as population_totale
                    FROM statistiques s
                    JOIN crimes c ON s.id_crime = c.id_crime
                    JOIN departements d ON s.code_departement = d.code_departement
                    WHERE 
                        c.type_crime = %s 
                        AND c.annee = %s
                    GROUP BY d.code_region, c.type_crime, c.annee
                    ORDER BY total_faits DESC
                """,
                "params": ["type_crime", "annee"],
            },
            "Top 10 départements": {
                "query": """
                    SELECT 
                        d.code_departement,
                        d.code_region,
                        c.type_crime,
                        c.nombre_faits,
                        s.taux_pour_mille,
                        d.population,
                        (c.nombre_faits / d.population * 100000) as ratio_population
                    FROM statistiques s
                    JOIN crimes c ON s.id_crime = c.id_crime
                    JOIN departements d ON s.code_departement = d.code_departement
                    WHERE 
                        c.type_crime = %s 
                        AND c.annee = %s
                    ORDER BY s.taux_pour_mille DESC
                    LIMIT 10
                """,
                "params": ["type_crime", "annee"],
            },
            "Analyse densité logements": {
                "query": """
                    SELECT 
                        d.code_departement,
                        c.type_crime,
                        c.nombre_faits,
                        d.logements,
                        d.population,
                        (d.logements / d.population) as ratio_logements,
                        s.taux_pour_mille
                    FROM statistiques s
                    JOIN crimes c ON s.id_crime = c.id_crime
                    JOIN departements d ON s.code_departement = d.code_departement
                    WHERE c.annee = %s
                    ORDER BY ratio_logements DESC
                """,
                "params": ["annee"],
            },
        }

    @staticmethod
    def validate_query(query: str) -> bool:
        """
        Basic SQL injection prevention
        Returns True if query seems safe, False otherwise
        """
        forbidden = [
            "DROP",
            "DELETE",
            "TRUNCATE",
            "INSERT",
            "UPDATE",
            "CREATE",
            "ALTER",
        ]
        upper_query = query.upper()
        return not any(word in upper_query for word in forbidden)
