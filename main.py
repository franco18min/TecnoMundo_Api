from app import create_app
import os

# Este bloque es el "botón de encendido" de nuestro servidor.
if __name__ == '__main__':
    config_name = os.getenv('FLASK_CONFIG', 'development')
    app = create_app(config_name)

    # Inicia el servidor de desarrollo de Flask.
    app.run(host='0.0.0.0', port=5000)
