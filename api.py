from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
import pandas as pd
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# =============================================================================
# CONFIGURACIÓN
# =============================================================================
CARPETA_DATOS = "archive_final"  # Nueva carpeta para los datos
COLUMNAS_REQUERIDAS = ['Categoria', 'Nombre del Producto', 'Cantidad']
EXTENSIONES_VALIDAS = ['csv', 'xlsx']

# =============================================================================
# CARGA DE DATOS (MODIFICADA PARA CARPETA ESPECÍFICA)
# =============================================================================
try:
    logger.info("=" * 60)
    logger.info("INICIANDO CARGA DE DATOS")
    logger.info("=" * 60)

    # Verificar existencia de la carpeta
    if not os.path.exists(CARPETA_DATOS):
        raise FileNotFoundError(f"No existe la carpeta '{CARPETA_DATOS}'")

    df = None
    archivo_encontrado = None

    # Buscar en la carpeta específica
    for filename in os.listdir(CARPETA_DATOS):
        if filename.split('.')[-1].lower() in EXTENSIONES_VALIDAS:
            try:
                ruta_completa = os.path.join(CARPETA_DATOS, filename)
                logger.info(f"Intentando cargar: {ruta_completa}")

                if filename.endswith('.csv'):
                    # Leer CSV
                    try:
                        temp_df = pd.read_csv(ruta_completa, delimiter=',', encoding='utf-8')
                    except:
                        temp_df = pd.read_csv(ruta_completa, delimiter=';', encoding='latin-1')
                else:
                    # Leer Excel
                    temp_df = pd.read_excel(ruta_completa, engine='openpyxl')

                # Validar columnas
                if all(col in temp_df.columns for col in COLUMNAS_REQUERIDAS):
                    df = temp_df
                    archivo_encontrado = filename
                    break

            except Exception as e:
                logger.warning(f"Error al procesar {filename}: {str(e)}")
                continue

    if df is None:
        raise ValueError(f"No se encontraron archivos válidos en '{CARPETA_DATOS}' con columnas requeridas")

    # Limpieza de datos
    df['Categoria'] = df['Categoria'].str.strip().str.lower()
    df['Cantidad'] = pd.to_numeric(df['Cantidad'], errors='coerce').fillna(0)
    df = df.dropna(subset=COLUMNAS_REQUERIDAS)

    logger.info(f"\n✅ DATOS CARGADOS DESDE: {os.path.join(CARPETA_DATOS, archivo_encontrado)}")
    logger.info(f"📦 Registros totales: {len(df)}")
    logger.info(f"🏷️ Categorías únicas: {df['Categoria'].nunique()}")

except Exception as e:
    logger.error("\n❌ ERROR EN CARGA DE DATOS:")
    logger.error(f"Mensaje: {str(e)}")
    logger.error(
        f"Archivos en '{CARPETA_DATOS}': {os.listdir(CARPETA_DATOS) if os.path.exists(CARPETA_DATOS) else 'No existe'}")
    raise


# =============================================================================
# RUTAS (MANTIENE LA MISMA FUNCIONALIDAD)
# =============================================================================
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/categorias')
def get_categorias():
    try:
        categorias = sorted(df['Categoria'].str.title().unique().tolist())
        return jsonify(categorias)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/top-productos')
def get_top_productos():
    try:
        categoria = request.args.get('categoria', '').strip().lower()

        if not categoria:
            return jsonify({"error": "Parámetro 'categoria' requerido"}), 400

        filtered = df[df['Categoria'] == categoria]

        if filtered.empty:
            return jsonify({"error": f"Categoría '{categoria.title()}' no encontrada"}), 404

        top_productos = (
            filtered.nlargest(10, 'Cantidad')
            [['Nombre del Producto', 'Cantidad']]
            .sort_values('Cantidad', ascending=False)
        )

        return jsonify({
            "productos": top_productos['Nombre del Producto'].tolist(),
            "cantidades": top_productos['Cantidad'].tolist(),
            "categoria": categoria.title()
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)