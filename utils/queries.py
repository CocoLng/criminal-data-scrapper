from typing import Dict

class QueryBuilder:
    @staticmethod
    def get_predefined_queries() -> Dict[str, str]:
        """Returns a dictionary of predefined queries with their descriptions"""
        return {
            "Statistiques par département": """
                SELECT d.code_departement, c.type_crime, c.annee, s.taux_pour_mille
                FROM statistiques s
                JOIN crimes c ON s.id_crime = c.id_crime
                JOIN departements d ON s.code_departement = d.code_departement
                WHERE c.type_crime = %s AND c.annee = %s
            """,
            
            "Evolution annuelle par type": """
                SELECT c.annee, c.type_crime, SUM(c.nombre_faits) as total_faits
                FROM crimes c
                WHERE c.type_crime = %s
                GROUP BY c.annee, c.type_crime
                ORDER BY c.annee
            """,
            
            "Top départements par crime": """
                SELECT d.code_departement, c.type_crime, s.taux_pour_mille
                FROM statistiques s
                JOIN crimes c ON s.id_crime = c.id_crime
                JOIN departements d ON s.code_departement = d.code_departement
                WHERE c.annee = %s
                ORDER BY s.taux_pour_mille DESC
                LIMIT 10
            """
        }

    @staticmethod
    def validate_query(query: str) -> bool:
        """
        Basic SQL injection prevention
        Returns True if query seems safe, False otherwise
        """
        # Liste de mots-clés interdits pour les requêtes utilisateur
        forbidden = ['DROP', 'DELETE', 'TRUNCATE', 'INSERT', 'UPDATE', 'CREATE', 'ALTER']
        
        # Convertir en majuscules pour la vérification
        upper_query = query.upper()
        
        # Vérifier si des mots interdits sont présents
        return not any(word in upper_query for word in forbidden)