import pandas as pd
import os
import logging
from threading import Lock

logger = logging.getLogger(__name__)
_state = {"df": pd.DataFrame(), "info": {"datos_cargados": False, "error_carga": "No se han cargado datos."}}
_lock = Lock()


def _detectar_columna_cantidad(df):
    posibles_columnas = ['Cantidad', 'Ventas', 'Total', 'Qty', 'Units', 'Volume']
    for col_base in posibles_columnas:
        for col_variant in [col_base, col_base.lower(), col_base.upper()]:
            if col_variant in df.columns: return col_variant
    columnas_numericas = df.select_dtypes(include=['number']).columns
    if len(columnas_numericas) > 0: return columnas_numericas[0]
    return None


def _leer_archivo(ruta_archivo):
    extension = os.path.splitext(ruta_archivo)[1].lower()
    try:
        if extension == '.csv':
            return pd.read_csv(ruta_archivo, sep=None, engine='python', encoding='utf-8', on_bad_lines='skip')
        elif extension in ['.xlsx', '.xls']:
            return pd.read_excel(ruta_archivo)
        elif extension == '.json':
            return pd.read_json(ruta_archivo)
    except Exception as e:
        logger.error(f"Error crítico leyendo archivo {ruta_archivo}: {e}")
    return None


def load_and_process_data(data_folder):
    print(f">>> [load_and_process_data] - Iniciando. Buscando en: {data_folder}")
    with _lock:
        if not os.path.isdir(data_folder):
            msg = f"La carpeta de datos '{data_folder}' no existe."
            _state["info"] = {"datos_cargados": False, "error_carga": msg}
            return

        archivos_validos = [f for f in os.listdir(data_folder) if
                            f.split('.')[-1].lower() in ['csv', 'xlsx', 'xls', 'json']]
        if not archivos_validos:
            msg = f"No se encontraron archivos de datos válidos en '{data_folder}'."
            _state["info"] = {"datos_cargados": False, "error_carga": msg}
            return

        df_cargado = None
        archivo_origen = None
        col_cantidad_detectada = None

        for filename in archivos_validos:
            print(f">>> [load_and_process_data] - Intentando leer el archivo: {filename}")
            temp_df = _leer_archivo(os.path.join(data_folder, filename))
            if temp_df is None:
                continue

            temp_df.columns = [str(c).strip() for c in temp_df.columns]
            if 'Categoria' in temp_df.columns: temp_df.rename(columns={'Categoria': 'Categoría'}, inplace=True)
            col_producto = next((c for c in ['Nombre del Producto', 'Producto', 'Item'] if c in temp_df.columns), None)

            if 'Categoría' in temp_df.columns and col_producto:
                temp_df.rename(columns={col_producto: 'Nombre del Producto'}, inplace=True)
                col_cantidad_detectada = _detectar_columna_cantidad(temp_df)
                if col_cantidad_detectada:
                    df_cargado = temp_df
                    archivo_origen = filename
                    print(f">>> [load_and_process_data] - Archivo '{filename}' es válido y ha sido cargado.")
                    break

        if df_cargado is None:
            msg = "No se encontró un archivo con la estructura requerida (Categoría, Producto, Cantidad)."
            _state["info"] = {"datos_cargados": False, "error_carga": msg}
            return

        print(">>> [load_and_process_data] - Iniciando procesamiento final de datos...")
        df = df_cargado.copy()
        df['Categoría'] = df['Categoría'].astype(str).str.strip().str.lower()
        df['Nombre del Producto'] = df['Nombre del Producto'].astype(str).str.strip()
        df[col_cantidad_detectada] = pd.to_numeric(df[col_cantidad_detectada], errors='coerce').fillna(0)
        df.dropna(subset=['Categoría', 'Nombre del Producto'], inplace=True)
        df = df[df[col_cantidad_detectada] > 0]

        df_agrupado = df.groupby(['Categoría', 'Nombre del Producto'])[col_cantidad_detectada].sum().reset_index()
        df_agrupado.rename(columns={col_cantidad_detectada: 'Cantidad'}, inplace=True)

        _state['df'] = df_agrupado
        _state['info'] = {
            "datos_cargados": True, "error_carga": None,
            "total_productos": len(df_agrupado),
            "total_categorias": df_agrupado['Categoría'].nunique(),
            "total_unidades": int(df_agrupado['Cantidad'].sum()),
            "archivo_origen": archivo_origen,
        }
        print(">>> [load_and_process_data] - Procesamiento finalizado exitosamente.")


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