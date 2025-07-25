import os
from pathlib import Path
from dotenv import load_dotenv


# --- CORRECCIÓN CRÍTICA ---
# Esta función se ejecuta inmediatamente para cargar las variables de entorno.
# Debe ejecutarse ANTES de importar cualquier módulo de tu aplicación (como 'app.api').
def find_and_load_dotenv():
    """
    Busca el archivo .env en la ruta 'conf/env/.env' y lo carga.
    """
    # La ruta base es la carpeta que contiene este archivo main.py
    current_path = Path(__file__).resolve().parent
    dotenv_path = current_path / 'conf' / 'env' / '.env'

    if dotenv_path.exists():
        print(f"Cargando variables de entorno desde: {dotenv_path}")
        load_dotenv(dotenv_path=dotenv_path)
        # Verificación para depuración
        if not os.getenv("DATABRICKS_SERVER_HOSTNAME"):
            print("ADVERTENCIA: El archivo .env fue encontrado pero las variables no se cargaron correctamente.")
    else:
        print(
            f"ADVERTENCIA: No se encontró el archivo .env en la ruta esperada: {dotenv_path}. La aplicación podría fallar.")
        # Intenta cargar desde la raíz si no lo encuentra en la ruta específica
        load_dotenv()


# Ejecutar la carga de credenciales AHORA.
find_and_load_dotenv()

# Ahora que las variables están cargadas, podemos importar los módulos de la app de forma segura.
from flask import Flask
from app.api import api_bp


def create_app():
    """
    Crea y configura la aplicación Flask.
    """
    app = Flask(__name__)

    app.register_blueprint(api_bp, url_prefix='/api')

    @app.route('/')
    def index():
        return "API de TecnoMundo - Accede a /api/ para ver el dashboard."

    return app


if __name__ == '__main__':
    app = create_app()
    port = int(os.environ.get("PORT", 5001))
    app.run(debug=True, host='0.0.0.0', port=port)
