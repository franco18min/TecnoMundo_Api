print(">>> [main.py] - Iniciando script...")
import os
import traceback

try:
    print(">>> [main.py] - [Paso 1/5] Importando 'create_app' desde la carpeta 'app'...")
    from app import create_app

    print(">>> [main.py] - [Paso 1/5] Importación de 'create_app' exitosa.")

    if __name__ == '__main__':
        print(">>> [main.py] - [Paso 2/5] Entrando al bloque de ejecución principal.")

        config_name = os.getenv('FLASK_CONFIG', 'development')
        print(f">>> [main.py] - [Paso 2/5] Usando configuración: '{config_name}'")

        print(">>> [main.py] - [Paso 3/5] Creando la instancia de la aplicación (sin cargar datos)...")
        app = create_app(config_name)
        print(">>> [main.py] - [Paso 3/5] Instancia de la aplicación creada exitosamente.")

        print(">>> [main.py] - [Paso 4/5] Entrando al contexto de la app para cargar datos...")
        with app.app_context():
            from app.services import data_service

            data_service.load_and_process_data(app.config['DATA_FOLDER'])
        print(">>> [main.py] - [Paso 4/5] Carga de datos finalizada.")

        print(">>> [main.py] - [Paso 5/5] La aplicación es válida. Iniciando el servidor web...")
        app.run(host='0.0.0.0', port=5000)

except Exception as e:
    print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("!!! OCURRIÓ UN ERROR CRÍTICO AL INICIAR LA APLICACIÓN !!!")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    traceback.print_exc()

finally:
    print("\n--- Fin del script ---")
    input("Presiona Enter para cerrar la ventana...")