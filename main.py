# main.py
import os
from dotenv import load_dotenv
from app import create_app

# Cargar variables de entorno desde el archivo .env
# (Asegúrate de que la ruta sea correcta para tu proyecto)
env_path = os.path.join(os.path.dirname(__file__), 'conf', 'env', '.env')
if os.path.exists(env_path):
    print(f"Cargando variables de entorno desde: {env_path}")
    load_dotenv(dotenv_path=env_path)

# Obtener el entorno de la variable de entorno, si no, usar 'default'
config_name = os.getenv('FLASK_CONFIG') or 'default'

# Crear la aplicación usando la factory
app = create_app(config_name)

if __name__ == '__main__':
    # Usar un puerto diferente a 5000 si es necesario
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port)