print(">>> [app/__init__.py] - Iniciando la lectura de este archivo...")

from flask import Flask, render_template
from config import config_by_name
import logging


def create_app(config_name='default'):
    print(">>> [create_app] - [Paso A] Creando objeto Flask...")
    app = Flask(__name__, template_folder='templates')
    app.config.from_object(config_by_name[config_name])
    print(">>> [create_app] - [Paso A] Exitoso.")

    print(">>> [create_app] - [Paso B] Importando y registrando componentes (sin cargar datos)...")
    from .api import api_bp
    from .errors import register_error_handlers
    app.register_blueprint(api_bp, url_prefix='/api')
    register_error_handlers(app)
    print(">>> [create_app] - [Paso B] Exitoso.")

    @app.route('/')
    def index():
        return render_template('dashboard.html')

    print(">>> [create_app] - [Paso C] Ruta principal '/' definida.")

    print(">>> [create_app] - [Paso D] Devolviendo la instancia de la app. Creación completa.")
    return app
