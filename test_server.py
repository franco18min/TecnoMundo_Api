from flask import Flask

# Creamos una aplicación Flask muy simple
app = Flask(__name__)

# Definimos una ruta para la página principal
@app.route('/')
def hello():
    return "¡El servidor de prueba funciona!"

# Este es el "botón de encendido"
if __name__ == '__main__':
    print(">>> Iniciando servidor de prueba...")
    app.run(host='0.0.0.0', port=5000)
    print(">>> El servidor de prueba se detuvo.")