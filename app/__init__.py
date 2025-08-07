# app/__init__.py
from flask import Flask, render_template
from config import config_by_name
from .extensions import cache

def create_app(config_name='default'):
    app = Flask(__name__)
    app.config.from_object(config_by_name[config_name])

    cache.init_app(app)

    # Registrar Blueprints
    from .api import api_bp, actions_bp # Importar el nuevo blueprint
    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(actions_bp, url_prefix='/api/actions') # Registrar el nuevo blueprint

    from .errors import register_error_handlers
    register_error_handlers(app)

    @app.route('/')
    def index():
        return render_template('dashboard.html')

    return app
