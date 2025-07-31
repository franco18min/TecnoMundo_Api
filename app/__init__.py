# app/__init__.py
from flask import Flask, render_template
from config import config_by_name
from .extensions import cache  # Importamos desde extensions

def create_app(config_name='default'):
    app = Flask(__name__)
    app.config.from_object(config_by_name[config_name])

    # 1. Inicializar extensiones
    # Vinculamos el objeto 'cache' con nuestra aplicación.
    # A partir de este punto, el caché está listo para ser usado.
    cache.init_app(app)

    # 2. Registrar Blueprints
    # Importamos las rutas DESPUÉS de que el caché esté listo.
    # Esto rompe el ciclo de importación.
    from .api import api_bp
    app.register_blueprint(api_bp, url_prefix='/api')

    # 3. Registrar manejadores de errores
    from .errors import register_error_handlers
    register_error_handlers(app)

    # 4. Registrar ruta principal de la app
    @app.route('/')
    def index():
        return render_template('dashboard.html')

    return app
