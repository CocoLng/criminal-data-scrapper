import logging
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class AppConfig:
    # Configuration du logging
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_FILE: str = "app.log"
    LOG_LEVEL: int = logging.INFO

    # Configuration des visualisations
    COLORS: Dict[str, str] = {
        "primary": "#1f77b4",
        "secondary": "#ff7f0e",
        "error": "#d62728",
        "success": "#2ca02c",
        "warning": "#bcbd22",
    }

    # Plage d'années disponibles
    YEARS_RANGE: List[int] = list(range(2016, 2024))

    # Configuration des graphiques
    PLOT_CONFIG = {
        "map": {
            "height": 600,
            "width": 800,
            "color_scale": "Viridis",
            "projection": "mercator",
        },
        "bar": {"height": 400, "width": 600, "orientation": "v", "opacity": 0.8},
        "line": {"height": 400, "width": 600, "line_shape": "linear", "markers": True},
        "scatter": {"height": 400, "width": 600, "opacity": 0.7, "marker_size": 8},
    }

    # Libellés pour les graphiques
    LABELS = {
        "faits": "Nombre de faits",
        "taux_pour_mille": "Taux pour 1000 habitants",
        "population": "Population",
        "logements": "Nombre de logements",
        "annee": "Année",
        "region": "Région",
        "classe": "Type de délit",
    }


@dataclass
class GradioConfig:
    # Configuration de l'interface Gradio
    THEME: str = "default"
    QUEUE: bool = True
    CONCURRENCY_LIMIT: int = 5
    MAX_THREADS: int = 4

    # Configuration des composants
    COMPONENTS = {
        "map_height": 600,
        "chart_height": 400,
        "sidebar_width": "300px",
        "main_content_width": "800px",
    }

    # Messages d'interface
    MESSAGES = {
        "welcome": "# Analyse de la Délinquance en France",
        "loading": "Chargement des données...",
        "error": "Une erreur est survenue",
        "no_data": "Aucune donnée disponible pour cette sélection",
    }


def setup_logging():
    """Configure le système de logging"""
    logging.basicConfig(
        level=AppConfig.LOG_LEVEL,
        format=AppConfig.LOG_FORMAT,
        handlers=[logging.FileHandler(AppConfig.LOG_FILE), logging.StreamHandler()],
    )
