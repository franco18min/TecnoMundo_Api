# config.py
import os

class Config:
    """Configuración base."""
    SECRET_KEY = os.environ.get('SECRET_KEY', 'un-secreto-muy-dificil-de-adivinar')
    DEBUG = False
    TESTING = False
    DATA_FOLDER = os.environ.get('DATA_FOLDER', 'archive_categorized')

    # --- Configuración del Caché ---
    CACHE_TYPE = 'SimpleCache'
    CACHE_DEFAULT_TIMEOUT = 300

class DevelopmentConfig(Config):
    """Configuración para desarrollo."""
    DEBUG = True

class ProductionConfig(Config):
    """Configuración para producción."""
    pass

config_by_name = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}