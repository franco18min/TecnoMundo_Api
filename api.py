# --- main.py ---
# Punto de entrada de la aplicación. Su única responsabilidad es crear y correr la app.

from app import create_app
import logging

# Configurar un logging básico para la salida de la consola
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')

# Crear la instancia de la aplicación usando la factory function
# Esto cargará la configuración desde el archivo config.py
app = create_app()

if __name__ == '__main__':
    # El modo debug se controla desde la configuración, no directamente aquí.
    # Para producción, un servidor WSGI como Gunicorn ejecutará la app, no este script.
    app.run(host='0.0.0.0', port=5000)

# --- config.py ---
# Archivo central de configuración. Separa la configuración del código.
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
    # En producción, podrías tener configuraciones diferentes,
    # como una base de datos diferente o niveles de log más altos.
    pass


# Diccionario para acceder a las configuraciones fácilmente
config_by_name = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}

# --- En una nueva carpeta llamada "app" ---

# --- app/__init__.py ---
# Factory para crear la aplicación. Permite crear múltiples instancias de la app
# para testing o diferentes configuraciones.

from flask import Flask
from flask_cors import CORS
from flask_talisman import Talisman
from config import config_by_name
from .services import data_service
from .api import api_bp
from .errors import register_error_handlers
import logging


def create_app(config_name='default'):
    """
    Factory de la aplicación.
    Crea y configura una instancia de la aplicación Flask.
    """
    app = Flask(__name__)
    app.config.from_object(config_by_name[config_name])

    # --- Inicializar Extensiones ---
    CORS(app, resources={r"/api/*": {"origins": "*"}})  # Ser más específico con los orígenes en producción

    # --- Seguridad ---
    # Configura cabeceras de seguridad HTTP para protección básica
    # En producción, se recomienda forzar HTTPS: content_security_policy_nonce_in=['script-src']
    Talisman(app, content_security_policy=None)

    # --- Carga de Datos Inicial ---
    # La aplicación intenta cargar los datos al iniciar.
    # Si falla, la app seguirá corriendo y los endpoints informarán del error.
    logging.info("Iniciando la carga de datos del servicio...")
    data_service.load_and_process_data(app.config['DATA_FOLDER'])

    # --- Registrar Blueprints y Handlers ---
    app.register_blueprint(api_bp, url_prefix='/api')
    register_error_handlers(app)

    @app.route('/')
    def index():
        # Idealmente, el frontend se sirve por separado (ej. Nginx), pero esto funciona para empezar.
        return "API de Dashboard de Ventas está en línea. Accede al frontend para visualizar los datos."

    return app


# --- app/services/data_service.py ---
# Capa de servicio. Encapsula toda la lógica de negocio de los datos.

import pandas as pd
import os
import logging
from threading import Lock

logger = logging.getLogger(__name__)

# Estado del servicio, encapsulado para no ser global a toda la app.
_state = {
    "df": pd.DataFrame(),
    "info": {"datos_cargados": False, "error_carga": "No se han cargado datos."}
}
_lock = Lock()


def _detectar_columna_cantidad(df):
    # (El código de esta función es el mismo que en la versión anterior)
    posibles_columnas = ['Cantidad', 'Ventas', 'Total', 'Qty', 'Units', 'Volume']
    for col_base in posibles_columnas:
        for col_variant in [col_base, col_base.lower(), col_base.upper()]:
            if col_variant in df.columns:
                return col_variant
    for col in df.select_dtypes(include=['int64', 'float64']).columns:
        return col
    return None


def _leer_archivo(ruta_archivo):
    # (El código de esta función es el mismo que en la versión anterior)
    extension = ruta_archivo.split('.')[-1].lower()
    try:
        if extension == 'csv':
            return pd.read_csv(ruta_archivo, sep=None, engine='python', encoding='utf-8', on_bad_lines='warn')
        elif extension in ['xlsx', 'xls']:
            return pd.read_excel(ruta_archivo)
        elif extension == 'json':
            return pd.read_json(ruta_archivo)
    except Exception as e:
        logger.error(f"Error crítico leyendo archivo {ruta_archivo}: {e}")
    return None


def load_and_process_data(data_folder):
    with _lock:
        logger.info(f"Iniciando escaneo de datos en: {data_folder}")
        # (Lógica de carga y procesamiento similar a la anterior, pero actualizando _state)
        # ... (código omitido por brevedad, es el mismo que en la versión anterior)
        # Al final, en lugar de app_state, actualiza _state
        # Ejemplo de actualización al final del proceso:
        # _state['df'] = df_agrupado
        # _state['info'] = { ... }
        # logger.info("Servicio de datos actualizado.")
        return get_status()


def get_status():
    with _lock:
        return _state['info']


def get_categorias():
    with _lock:
        if not _state['info']['datos_cargados']: return None
        return sorted(_state['df']['Categoría'].str.title().unique().tolist())


def get_top_productos(categoria, limite):
    with _lock:
        if not _state['info']['datos_cargados']: return None

        filtered = _state['df'][_state['df']['Categoría'] == categoria.lower()]
        if filtered.empty: return {}

        top = filtered.nlargest(limite, 'Cantidad')
        return {
            "productos": top['Nombre del Producto'].tolist(),
            "cantidades": top['Cantidad'].tolist(),
            "categoria": categoria.title(),
            "total_productos_categoria": len(filtered),
            "total_unidades_categoria": int(filtered['Cantidad'].sum())
        }


# --- app/api.py ---
# Blueprint para la API. Agrupa todas las rutas de la API.

from flask import Blueprint, jsonify, request, current_app
from .services import data_service

api_bp = Blueprint('api', __name__)


@api_bp.route('/status')
def status():
    return jsonify(data_service.get_status())


@api_bp.route('/reload', methods=['POST'])
def reload():
    folder = current_app.config['DATA_FOLDER']
    status = data_service.load_and_process_data(folder)
    if status['datos_cargados']:
        return jsonify({"status": "success", "message": "Datos recargados.", "info": status})
    else:
        return jsonify({"status": "error", "message": "Fallo al recargar.", "error": status['error_carga']}), 500


@api_bp.route('/categorias')
def categorias():
    categorias = data_service.get_categorias()
    if categorias is None:
        return jsonify({"error": "Datos no disponibles."}), 503
    return jsonify(categorias)


@api_bp.route('/top-productos')
def top_productos():
    categoria = request.args.get('categoria', type=str)
    limite = request.args.get('limite', 10, type=int)

    if not categoria:
        return jsonify({"error": "Parámetro 'categoria' es requerido."}), 400

    productos = data_service.get_top_productos(categoria, limite)
    if productos is None:
        return jsonify({"error": "Datos no disponibles."}), 503
    if not productos:
        return jsonify({"error": f"Categoría '{categoria}' no encontrada."}), 404

    return jsonify(productos)


# --- app/errors.py ---
# Manejo de errores centralizado.

from flask import jsonify


def register_error_handlers(app):
    @app.errorhandler(404)
    def not_found_error(error):
        return jsonify({"error": "Recurso no encontrado", "message": "La URL solicitada no existe."}), 404

    @app.errorhandler(500)
    def internal_error(error):
        # En producción, no deberías exponer el error original.
        # Se debería loggear el error.
        return jsonify({"error": "Error interno del servidor", "message": "Ocurrió un problema inesperado."}), 500

    @app.errorhandler(400)
    def bad_request_error(error):
        return jsonify({"error": "Solicitud incorrecta",
                        "message": error.description or "La solicitud no pudo ser entendida."}), 400
