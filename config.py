import os

class Config:
    """Configuración base."""
    SECRET_KEY = os.environ.get('SECRET_KEY', 'un-secreto-muy-dificil-de-adivinar')
    DEBUG = False
    TESTING = False
    # Carpeta donde se buscarán los datos
    DATA_FOLDER = os.environ.get('DATA_FOLDER', 'archive_categorized')

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

