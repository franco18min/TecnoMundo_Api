from flask import Flask, render_template
from config import config_by_name
import logging


def create_app(config_name='default'):
    """
    Factory de la aplicación. Construye y configura la instancia de Flask.
    """
    app = Flask(__name__, template_folder='templates')
    app.config.from_object(config_by_name[config_name])

    # Importar componentes de la app aquí para evitar importaciones circulares.
    from .services import data_service
    from .api import api_bp
    from .errors import register_error_handlers

    # Carga los datos iniciales de forma segura dentro del contexto de la aplicación.
    with app.app_context():
        logging.info("Cargando datos iniciales del servicio...")
        data_service.load_and_process_data(app.config['DATA_FOLDER'])
        logging.info("Carga de datos inicial finalizada.")

    # Registrar las rutas de la API y los manejadores de errores.
    app.register_blueprint(api_bp, url_prefix='/api')
    register_error_handlers(app)

    # Definir la ruta principal que servirá el dashboard.
    @app.route('/')
    def index():
        return render_template('dashboard.html')

    return app