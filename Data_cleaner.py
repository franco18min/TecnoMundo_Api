
"""
Data Cleaner - Módulo para limpieza y procesamiento de datos
Procesa archivos CSV y Excel, limpia datos y genera archivos procesados
"""

import pandas as pd
import numpy as np
import os
import math
import re
from datetime import datetime
from typing import Optional, List, Tuple, Dict
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataCleaner:
    """Clase principal para la limpieza y procesamiento de datos"""
    
    def __init__(self, directorio_origen: str = "archive_original", 
                 directorio_destino: str = "archive_processed"):
        """
        Inicializar el limpiador de datos
        
        Args:
            directorio_origen: Directorio donde se encuentran los archivos originales
            directorio_destino: Directorio donde se guardarán los archivos procesados
        """
        self.directorio_origen = directorio_origen
        self.directorio_destino = directorio_destino
        self.directorio_problemas = "Problems_to_solve"  # Nueva carpeta para problemas
        self.extensiones_soportadas = ['.csv', '.xlsx', '.xls']
        self.df = None
        self.df_original = None  # Para comparaciones
        self.filas_problematicas = []
        self.columnas_problematicas = {}  # Nuevo: almacena info de columnas problemáticas
        self.info_carga_archivo = {}  # Información sobre cómo se cargó el archivo
        
    def _validar_directorios(self) -> None:
        """Valida y crea directorios necesarios"""
        if not os.path.exists(self.directorio_origen):
            raise FileNotFoundError(f"El directorio '{self.directorio_origen}' no existe")
            
        # Crear directorio de destino si no existe
        if not os.path.exists(self.directorio_destino):
            os.makedirs(self.directorio_destino)
            logger.info(f"Directorio de destino creado: {self.directorio_destino}")
        
        # Crear directorio de problemas si no existe
        if not os.path.exists(self.directorio_problemas):
            os.makedirs(self.directorio_problemas)
            logger.info(f"Directorio de problemas creado: {self.directorio_problemas}")
    
    def _obtener_archivos_validos(self) -> List[str]:
        """
        Obtiene lista de archivos válidos en el directorio origen
        
        Returns:
            Lista de nombres de archivos válidos
        """
        try:
            archivos = os.listdir(self.directorio_origen)
            archivos_validos = [
                archivo for archivo in archivos 
                if any(archivo.lower().endswith(ext) for ext in self.extensiones_soportadas)
            ]
            
            if not archivos_validos:
                raise ValueError(f"No se encontraron archivos válidos en '{self.directorio_origen}'")
                
            return archivos_validos
        except Exception as e:
            logger.error(f"Error al obtener archivos: {e}")
            raise
    
    def _detectar_fila_encabezados_excel(self, ruta_archivo: str, max_filas_buscar: int = 15) -> Tuple[int, pd.DataFrame]:
        """
        Detecta automáticamente la fila que contiene los encabezados reales en un archivo Excel
        
        Args:
            ruta_archivo: Ruta completa del archivo Excel
            max_filas_buscar: Número máximo de filas a analizar para encontrar encabezados
            
        Returns:
            Tupla con (fila_encabezados, dataframe_cargado)
        """
        logger.info("🔍 Detectando fila de encabezados en archivo Excel...")
        
        # Leer las primeras filas sin especificar encabezados
        df_preview = pd.read_excel(ruta_archivo, header=None, nrows=max_filas_buscar, engine='openpyxl')
        
        logger.info(f"📊 Analizando las primeras {len(df_preview)} filas del archivo:")
        
        mejor_fila = 0
        mejor_score = 0
        info_analisis = []
        
        for fila_idx in range(len(df_preview)):
            fila_actual = df_preview.iloc[fila_idx]
            
            # Convertir valores a string para análisis
            valores_fila = fila_actual.astype(str).tolist()
            
            # Saltar filas completamente vacías
            valores_no_vacios = [v for v in valores_fila if v not in ['nan', '', 'None', None]]
            if len(valores_no_vacios) == 0:
                logger.info(f"  Fila {fila_idx}: VACÍA - Saltando")
                continue
            
            # Calcular score de la fila como posible encabezado
            score = self._calcular_score_encabezado(valores_fila, fila_idx)
            
            info_fila = {
                'fila': fila_idx,
                'valores': valores_no_vacios[:8],  # Primeros 8 valores para mostrar
                'score': score,
                'es_candidato': score > 8  # Aumenté el umbral
            }
            info_analisis.append(info_fila)
            
            logger.info(f"  Fila {fila_idx}: Score={score:.1f} | Valores: {valores_no_vacios[:3]}...")
            
            if score > mejor_score:
                mejor_score = score
                mejor_fila = fila_idx
        
        # NUEVO: Si la primera fila tiene un score muy bajo, buscar una mejor
        primera_fila_score = next((item['score'] for item in info_analisis if item['fila'] == 0), 0)
        
        # Si encontramos una fila significativamente mejor que la primera
        if mejor_score > primera_fila_score + 5:  # Diferencia mínima de 5 puntos
            logger.info(f"🔄 Fila {mejor_fila} es significativamente mejor que fila 0")
            logger.info(f"   Score fila 0: {primera_fila_score:.1f} vs Score fila {mejor_fila}: {mejor_score:.1f}")
        else:
            # Si no hay diferencia significativa, usar fila 0
            mejor_fila = 0
            mejor_score = primera_fila_score
        
        # Guardar información del análisis
        self.info_carga_archivo['analisis_encabezados'] = info_analisis
        self.info_carga_archivo['fila_encabezados_detectada'] = mejor_fila
        self.info_carga_archivo['score_mejor_fila'] = mejor_score
        
        logger.info(f"✅ Fila de encabezados seleccionada: Fila {mejor_fila} (Score: {mejor_score:.1f})")
        
        # Cargar archivo con la fila de encabezados correcta
        try:
            if mejor_fila == 0:
                df_final = pd.read_excel(ruta_archivo, engine='openpyxl')
            else:
                # IMPORTANTE: Usar skiprows en lugar de header para saltar las filas anteriores
                df_final = pd.read_excel(ruta_archivo, skiprows=mejor_fila, engine='openpyxl')
                
                # Verificar que las columnas no sean genéricas
                if self._tiene_columnas_genericas(df_final):
                    logger.warning(f"⚠️ Aún hay columnas genéricas después de usar fila {mejor_fila}")
                    # Intentar la fila siguiente
                    for intento_fila in range(mejor_fila + 1, min(mejor_fila + 3, max_filas_buscar)):
                        df_intento = pd.read_excel(ruta_archivo, skiprows=intento_fila, engine='openpyxl')
                        if not self._tiene_columnas_genericas(df_intento):
                            logger.info(f"✅ Usando fila {intento_fila} en su lugar")
                            df_final = df_intento
                            mejor_fila = intento_fila
                            break
                    
        except Exception as e:
            logger.warning(f"Error al cargar con fila {mejor_fila}: {e}")
            logger.info("Recargando con fila 0 como respaldo")
            df_final = pd.read_excel(ruta_archivo, engine='openpyxl')
            mejor_fila = 0
        
        # Actualizar información final
        self.info_carga_archivo['fila_encabezados_final'] = mejor_fila
        
        return mejor_fila, df_final
    
    def _tiene_columnas_genericas(self, df: pd.DataFrame) -> bool:
        """
        Verifica si el DataFrame tiene muchas columnas con nombres genéricos
        
        Args:
            df: DataFrame a verificar
            
        Returns:
            True si tiene muchas columnas genéricas
        """
        if df.empty:
            return True
            
        columnas_genericas = 0
        for col in df.columns:
            col_str = str(col)
            # Detectar columnas genéricas comunes
            if (col_str.startswith('Unnamed') or 
                col_str.startswith('C') and col_str[1:].isdigit() or
                col_str in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'] or
                col_str.startswith('Column')):
                columnas_genericas += 1
        
        # Si más del 60% son genéricas, es problemático
        porcentaje_genericas = columnas_genericas / len(df.columns)
        return porcentaje_genericas > 0.6
    
    def _calcular_score_encabezado(self, valores_fila: List[str], indice_fila: int) -> float:
        """
        Calcula un score para determinar si una fila contiene encabezados válidos
        
        Args:
            valores_fila: Lista de valores de la fila convertidos a string
            indice_fila: Índice de la fila (0-based)
            
        Returns:
            Score numérico (mayor = mejor candidato a encabezado)
        """
        score = 0.0
        
        # 1. Filtrar valores no vacíos
        valores_no_vacios = [v for v in valores_fila if v not in ['nan', '', 'None', None]]
        if len(valores_no_vacios) == 0:
            return 0.0

        # 2. Bonus por cantidad de valores no vacíos
        score += len(valores_no_vacios) * 2.0

        # 3. BONUS FUERTE por palabras típicas de encabezados
        palabras_encabezado = [
            'fecha', 'date', 'nombre', 'producto', 'cantidad', 'precio', 'total',
            'subtotal', 'codigo', 'código', 'descripcion', 'descripción', 'cliente',
            'comprobante', 'ganancia', 'categoria', 'categoría', 'tipo', 'estado',
            'id', 'num', 'número', 'un', 'unidad', 'valor'
        ]

        for valor in valores_no_vacios:
            valor_lower = valor.lower()
            for palabra_clave in palabras_encabezado:
                if palabra_clave in valor_lower:
                    score += 5.0  # Bonus más alto
                    break

        # 4. BONUS por patrones típicos de encabezados
        for valor in valores_no_vacios:
            # Bonus si contiene palabras separadas por espacios (típico de encabezados)
            if ' ' in valor and len(valor.split()) <= 5:
                score += 2.0

            # Bonus si parece un nombre de columna y no un valor de datos
            if not self._parece_valor_datos(valor):
                score += 2.0

            # Bonus especial para patrones como "Nombre del Producto", "Precio Un.", etc.
            if any(patron in valor.lower() for patron in ['del ', 'de ', 'un.', 'nº', 'num']):
                score += 3.0

        # 5. PENALIZAR FUERTE si muchos valores parecen números o fechas
        valores_que_parecen_datos = sum(1 for v in valores_no_vacios if self._parece_valor_datos(v))
        if valores_que_parecen_datos > len(valores_no_vacios) * 0.6:
            score -= 8.0  # Penalización más fuerte

        # 6. Bonus moderado por posición
        if indice_fila == 0:
            score += 3.0
        elif indice_fila == 1:
            score += 2.0
        elif indice_fila > 8:
            score -= 2.0

        # 7. Penalizar filas con muchos valores duplicados
        if len(set(valores_no_vacios)) < len(valores_no_vacios) * 0.7:
            score -= 4.0

        # 8. NUEVO: Bonus por tener estructura típica de encabezados de ventas
        estructura_ventas = ['fecha', 'producto', 'cantidad', 'precio', 'total']
        palabras_en_fila = ' '.join(valores_no_vacios).lower()
        coincidencias_ventas = sum(1 for palabra in estructura_ventas if palabra in palabras_en_fila)
        if coincidencias_ventas >= 3:
            score += 10.0  # Bonus muy alto para estructura de ventas

        return max(0.0, score)

    def _parece_valor_datos(self, valor: str) -> bool:
        """
        Determina si un valor parece ser un dato en lugar de un encabezado

        Args:
            valor: Valor a evaluar

        Returns:
            True si parece un valor de datos, False si parece un encabezado
        """
        valor = str(valor).strip()

        # Es número puro
        try:
            float(valor.replace(',', '.').replace('$', '').replace('€', ''))
            return True
        except:
            pass

        # Es fecha
        patrones_fecha = [
            r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
            r'\d{4}[/-]\d{1,2}[/-]\d{1,2}',
            r'\d{1,2}\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)',
        ]

        for patron in patrones_fecha:
            if re.match(patron, valor, re.IGNORECASE):
                return True

        # Es muy largo (probable descripción de producto)
        if len(valor) > 60:
            return True

        # Contiene muchos números y símbolos (códigos de producto)
        if re.search(r'^[A-Z0-9\-/\s]+$', valor) and len(valor) > 6:
            return True

        # Parece un código o ID
        if re.match(r'^[A-Z]{2,4}\d{3,}$', valor):
            return True

        return False

    def _cargar_archivo(self, nombre_archivo: str) -> pd.DataFrame:
        """
        Carga un archivo en un DataFrame con detección inteligente de encabezados

        Args:
            nombre_archivo: Nombre del archivo a cargar

        Returns:
            DataFrame con los datos del archivo
        """
        ruta_completa = os.path.join(self.directorio_origen, nombre_archivo)
        logger.info(f"📁 Cargando archivo: {ruta_completa}")

        # Inicializar información de carga
        self.info_carga_archivo = {
            'nombre_archivo': nombre_archivo,
            'ruta_completa': ruta_completa,
            'tipo_archivo': None,
            'metodo_carga': None,
            'problemas_detectados': []
        }

        try:
            if nombre_archivo.lower().endswith('.csv'):
                self.info_carga_archivo['tipo_archivo'] = 'CSV'
                self.info_carga_archivo['metodo_carga'] = 'pandas.read_csv'

                # Para CSV, carga normal
                df = pd.read_csv(ruta_completa, encoding='utf-8')
                logger.info(f"✅ Archivo CSV cargado correctamente con {df.shape[0]} filas y {df.shape[1]} columnas")

            elif nombre_archivo.lower().endswith(('.xlsx', '.xls')):
                self.info_carga_archivo['tipo_archivo'] = 'Excel'

                # Para Excel, detectar encabezados automáticamente
                fila_encabezados, df = self._detectar_fila_encabezados_excel(ruta_completa)

                self.info_carga_archivo['metodo_carga'] = f'pandas.read_excel (fila_encabezados={fila_encabezados})'

                logger.info(f"✅ Archivo Excel cargado correctamente:")
                logger.info(f"   - Fila de encabezados: {fila_encabezados}")
                logger.info(f"   - Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
                logger.info(f"   - Columnas: {list(df.columns)}")

            # Validar que el DataFrame tiene datos
            if df.empty:
                raise ValueError("El archivo está vacío o no contiene datos válidos")

            # Detectar problemas comunes en las columnas
            self._detectar_problemas_columnas_iniciales(df)

            self.info_carga_archivo['columnas_finales'] = list(df.columns)
            self.info_carga_archivo['dimensiones_finales'] = df.shape

            return df

        except Exception as e:
            self.info_carga_archivo['error'] = str(e)
            logger.error(f"❌ Error al cargar {nombre_archivo}: {e}")
            raise

    def _detectar_problemas_columnas_iniciales(self, df: pd.DataFrame) -> None:
        """
        Detecta problemas comunes en las columnas después de la carga

        Args:
            df: DataFrame a analizar
        """
        problemas = []

        # Detectar columnas con nombres genéricos (Unnamed, C0, C1, etc.)
        columnas_genericas = [col for col in df.columns if
                            (str(col).startswith('Unnamed') or
                             str(col).startswith('C') and str(col)[1:].isdigit() or
                             str(col) in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'])]

        if columnas_genericas:
            problemas.append(f"Columnas con nombres genéricos detectadas: {columnas_genericas}")
            logger.warning(f"⚠️ Detectadas columnas con nombres genéricos: {columnas_genericas}")

            # Si hay muchas columnas genéricas, es probable que los encabezados estén mal
            if len(columnas_genericas) > len(df.columns) * 0.5:
                problemas.append("Más del 50% de las columnas tienen nombres genéricos - posible problema de encabezados")
                logger.warning("⚠️ Posible problema: Los encabezados reales pueden estar en los datos")

        # Detectar columnas completamente vacías
        columnas_vacias = [col for col in df.columns if df[col].isna().all()]
        if columnas_vacias:
            problemas.append(f"Columnas completamente vacías: {columnas_vacias}")
            logger.warning(f"⚠️ Columnas completamente vacías: {columnas_vacias}")

        # Detectar primera fila que podría ser encabezados
        if not df.empty:
            primera_fila = df.iloc[0].astype(str).tolist()
            score_primera_fila = self._calcular_score_encabezado(primera_fila, 0)

            if score_primera_fila > 15.0:  # Score alto = probable encabezado
                problemas.append("La primera fila de datos parece contener encabezados")
                logger.warning("⚠️ La primera fila de datos parece contener encabezados reales")

        self.info_carga_archivo['problemas_detectados'] = problemas

        if problemas:
            logger.info(f"📋 Resumen de problemas detectados en carga:")
            for i, problema in enumerate(problemas, 1):
                logger.info(f"   {i}. {problema}")

    def imprimir_info_carga_archivo(self) -> None:
        """Imprime información detallada sobre cómo se cargó el archivo"""
        if not self.info_carga_archivo:
            print("❌ No hay información de carga disponible")
            return

        print("\n" + "="*70)
        print("📁 INFORMACIÓN DE CARGA DEL ARCHIVO")
        print("="*70)

        info = self.info_carga_archivo

        print(f"Archivo: {info['nombre_archivo']}")
        print(f"Tipo: {info['tipo_archivo']}")
        print(f"Método de carga: {info['metodo_carga']}")

        if 'dimensiones_finales' in info:
            print(f"Dimensiones: {info['dimensiones_finales'][0]} filas × {info['dimensiones_finales'][1]} columnas")

        if 'analisis_encabezados' in info:
            print(f"\n🔍 ANÁLISIS DE ENCABEZADOS:")
            print(f"Fila de encabezados detectada: {info['fila_encabezados_detectada']}")
            if 'fila_encabezados_final' in info:
                print(f"Fila de encabezados final: {info['fila_encabezados_final']}")
            print(f"Score de la mejor fila: {info['score_mejor_fila']:.1f}")

            print("\nAnálisis por fila:")
            for analisis_fila in info['analisis_encabezados']:
                estado = "✅ CANDIDATO" if analisis_fila['es_candidato'] else "❌ Descartado"
                print(f"  Fila {analisis_fila['fila']}: Score {analisis_fila['score']:.1f} - {estado}")
                print(f"    Valores: {analisis_fila['valores']}")

        if 'columnas_finales' in info:
            print(f"\n📊 COLUMNAS FINALES ({len(info['columnas_finales'])}):")
            for i, col in enumerate(info['columnas_finales']):
                print(f"  {i+1}. '{col}'")

        if info['problemas_detectados']:
            print(f"\n⚠️ PROBLEMAS DETECTADOS:")
            for i, problema in enumerate(info['problemas_detectados'], 1):
                print(f"  {i}. {problema}")
        else:
            print(f"\n✅ No se detectaron problemas en la carga")

        if 'error' in info:
            print(f"\n❌ ERROR: {info['error']}")

        print("="*70)

    def _es_columna_fecha(self, serie: pd.Series) -> bool:
        """
        Detecta si una columna contiene fechas

        Args:
            serie: Serie de pandas a evaluar

        Returns:
            True si es probable que sea una columna de fechas
        """
        # Verificar por nombre de columna
        palabras_fecha = ['fecha', 'date', 'día', 'dia', 'time', 'created', 'updated']
        if any(palabra in str(serie.name).lower() for palabra in palabras_fecha):
            return True

        # Convertir a string para evaluar formato
        serie_str = serie.astype(str).dropna()
        if len(serie_str) == 0:
            return False

        # Patrones de fecha comunes
        patrones_fecha = [
            r'\d{2}[/-]\d{2}[/-]\d{4}',      # dd/mm/yyyy o dd-mm-yyyy
            r'\d{4}[/-]\d{2}[/-]\d{2}',      # yyyy/mm/dd o yyyy-mm-dd
            r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}' # formatos variados
        ]

        # Si más del 30% coincide con algún patrón, es probablemente fecha
        for patron in patrones_fecha:
            coincidencias = serie_str.str.match(patron, na=False)
            if coincidencias.mean() > 0.3:
                return True

        return False

    def _es_columna_numerica(self, serie: pd.Series) -> bool:
        """
        Detecta si una columna es numérica

        Args:
            serie: Serie de pandas a evaluar

        Returns:
            True si más del 50% de los valores son numéricos
        """
        return pd.to_numeric(serie, errors='coerce').notna().mean() > 0.5

    def _analizar_columna_problematica(self, columna: str, mask_problematicos: pd.Series) -> Dict:
        """
        Analiza en detalle una columna problemática

        Args:
            columna: Nombre de la columna
            mask_problematicos: Máscara booleana de valores problemáticos

        Returns:
            Diccionario con análisis detallado de la columna
        """
        valores_problematicos = self.df_original[mask_problematicos][columna]
        valores_unicos_problematicos = valores_problematicos.value_counts()

        # Detectar tipo de columna
        if self._es_columna_fecha(self.df_original[columna]):
            tipo_columna = "fecha"
        elif self._es_columna_numerica(self.df_original[columna]):
            tipo_columna = "numérica"
        else:
            tipo_columna = "texto"

        analisis = {
            'nombre_columna': columna,
            'tipo_detectado': tipo_columna,
            'total_valores_problematicos': len(valores_problematicos),
            'porcentaje_problematicos': (len(valores_problematicos) / len(self.df_original)) * 100,
            'valores_unicos_problematicos': valores_unicos_problematicos.to_dict(),
            'indices_problematicos': valores_problematicos.index.tolist(),
            'muestras_valores_problematicos': valores_problematicos.head(10).tolist(),
            'estadisticas_generales': {
                'total_registros': len(self.df_original),
                'valores_nulos': self.df_original[columna].isna().sum(),
                'valores_vacios': (self.df_original[columna].astype(str) == '').sum(),
                'valores_unicos_totales': self.df_original[columna].nunique()
            }
        }

        return analisis

    def _procesar_columna_fecha(self, columna: str) -> None:
        """
        Procesa una columna de fechas

        Args:
            columna: Nombre de la columna a procesar
        """
        logger.info(f"Procesando columna de fecha: {columna}")

        # Identificar valores problemáticos
        mask_problematicos = (
            self.df[columna].isna() |
            (self.df[columna].astype(str) == '') |
            (self.df[columna].astype(str) == 'nan')
        )

        if mask_problematicos.any():
            self.filas_problematicas.extend(self.df[mask_problematicos].index.tolist())
            logger.info(f"  - Valores problemáticos encontrados: {mask_problematicos.sum()}")

            # Almacenar análisis detallado
            self.columnas_problematicas[columna] = self._analizar_columna_problematica(columna, mask_problematicos)

        # Convertir a datetime y manejar errores
        self.df[columna] = pd.to_datetime(self.df[columna], errors='coerce')

        # Rellenar valores nulos con fecha por defecto
        fecha_defecto = pd.Timestamp('1900-01-01')
        self.df[columna] = self.df[columna].fillna(fecha_defecto)

        # Formatear fecha
        self.df[columna] = self.df[columna].dt.strftime('%d-%m-%Y')

    def _procesar_columna_numerica(self, columna: str) -> None:
        """
        Procesa una columna numérica

        Args:
            columna: Nombre de la columna a procesar
        """
        logger.info(f"Procesando columna numérica: {columna}")

        # Identificar valores problemáticos
        mask_problematicos = (
            self.df[columna].isna() |
            (self.df[columna].astype(str) == '') |
            (self.df[columna].astype(str) == 'nan')
        )

        if mask_problematicos.any():
            self.filas_problematicas.extend(self.df[mask_problematicos].index.tolist())
            logger.info(f"  - Valores problemáticos encontrados: {mask_problematicos.sum()}")

            # Almacenar análisis detallado
            self.columnas_problematicas[columna] = self._analizar_columna_problematica(columna, mask_problematicos)

        # Convertir a numérico
        self.df[columna] = pd.to_numeric(self.df[columna], errors='coerce')

        # Rellenar con 0 y redondear hacia arriba
        self.df[columna] = self.df[columna].fillna(0)
        self.df[columna] = self.df[columna].apply(lambda x: math.ceil(x)).astype(int)

    def _procesar_columna_texto(self, columna: str) -> None:
        """
        Procesa una columna de texto

        Args:
            columna: Nombre de la columna a procesar
        """
        logger.info(f"Procesando columna de texto: {columna}")

        # Identificar valores problemáticos
        mask_problematicos = (
            self.df[columna].isna() |
            (self.df[columna].astype(str) == '') |
            (self.df[columna].astype(str) == 'nan')
        )

        if mask_problematicos.any():
            self.filas_problematicas.extend(self.df[mask_problematicos].index.tolist())
            logger.info(f"  - Valores problemáticos encontrados: {mask_problematicos.sum()}")

            # Almacenar análisis detallado
            self.columnas_problematicas[columna] = self._analizar_columna_problematica(columna, mask_problematicos)

        # Limpiar y normalizar texto
        self.df[columna] = self.df[columna].fillna('Sin registro')
        self.df[columna] = self.df[columna].astype(str)

        # Reemplazar valores problemáticos
        valores_problematicos = ['nan', '', 'null', 'None']
        for valor in valores_problematicos:
            self.df.loc[self.df[columna] == valor, columna] = 'Sin registro'

    def _limpiar_datos(self) -> None:
        """Limpia los datos del DataFrame según el tipo de cada columna"""
        logger.info("Iniciando limpieza de datos...")
        logger.info(f"Dimensiones originales: {self.df.shape}")

        # Guardar copia original para análisis
        self.df_original = self.df.copy()
        self.filas_problematicas = []
        self.columnas_problematicas = {}

        for columna in self.df.columns:
            if self._es_columna_fecha(self.df[columna]):
                self._procesar_columna_fecha(columna)
            elif self._es_columna_numerica(self.df[columna]):
                self._procesar_columna_numerica(columna)
            else:
                self._procesar_columna_texto(columna)

        # Eliminar duplicados de filas problemáticas
        self.filas_problematicas = sorted(list(set(self.filas_problematicas)))

        logger.info(f"Limpieza completada. Filas problemáticas identificadas: {len(self.filas_problematicas)}")
        logger.info(f"Columnas problemáticas identificadas: {len(self.columnas_problematicas)}")

        if self.filas_problematicas:
            self._mostrar_ejemplos_problematicos()

    def _mostrar_ejemplos_problematicos(self, max_ejemplos: int = 5) -> None:
        """
        Muestra ejemplos de filas problemáticas

        Args:
            max_ejemplos: Número máximo de ejemplos a mostrar
        """
        logger.info(f"Índices de filas problemáticas: {self.filas_problematicas}")
        logger.info("Ejemplos de filas problemáticas:")

        for i, idx in enumerate(self.filas_problematicas[:max_ejemplos]):
            logger.info(f"Fila {idx}:")
            logger.info(f"{self.df.iloc[idx].to_dict()}")
            if i < len(self.filas_problematicas[:max_ejemplos]) - 1:
                logger.info("---")

    def imprimir_analisis_columnas_problematicas(self, mostrar_detalle: bool = True) -> None:
        """
        Imprime análisis detallado de las columnas problemáticas

        Args:
            mostrar_detalle: Si mostrar análisis detallado o solo resumen
        """
        if not self.columnas_problematicas:
            print("\n" + "="*70)
            print("✅ NO SE ENCONTRARON COLUMNAS PROBLEMÁTICAS")
            print("="*70)
            return

        print("\n" + "="*70)
        print("🔍 ANÁLISIS DE COLUMNAS PROBLEMÁTICAS")
        print("="*70)

        for i, (columna, analisis) in enumerate(self.columnas_problematicas.items(), 1):
            print(f"\n📊 COLUMNA {i}: '{columna}'")
            print("-" * 50)
            print(f"Tipo detectado: {analisis['tipo_detectado'].upper()}")
            print(f"Total valores problemáticos: {analisis['total_valores_problematicos']}")
            print(f"Porcentaje problemático: {analisis['porcentaje_problematicos']:.2f}%")

            # Estadísticas generales
            stats = analisis['estadisticas_generales']
            print(f"Total registros: {stats['total_registros']}")
            print(f"Valores nulos: {stats['valores_nulos']}")
            print(f"Valores vacíos: {stats['valores_vacios']}")
            print(f"Valores únicos totales: {stats['valores_unicos_totales']}")

            if mostrar_detalle:
                print("\n🔎 VALORES PROBLEMÁTICOS ÚNICOS:")
                for valor, count in list(analisis['valores_unicos_problematicos'].items())[:10]:
                    print(f"  '{valor}': {count} ocurrencias")

                print(f"\n📝 MUESTRAS DE VALORES PROBLEMÁTICOS:")
                for j, valor in enumerate(analisis['muestras_valores_problematicos'][:5], 1):
                    print(f"  {j}. '{valor}'")

                print(f"\n📍 PRIMEROS ÍNDICES PROBLEMÁTICOS:")
                indices_muestra = analisis['indices_problematicos'][:10]
                print(f"  {indices_muestra}")

            if i < len(self.columnas_problematicas):
                print("\n" + "·" * 50)

        print("\n" + "="*70)

    def obtener_filas_problematicas_por_columna(self, nombre_columna: str, mostrar_filas: int = 10) -> pd.DataFrame:
        """
        Obtiene las filas problemáticas de una columna específica

        Args:
            nombre_columna: Nombre de la columna a analizar
            mostrar_filas: Número de filas a mostrar

        Returns:
            DataFrame con las filas problemáticas de esa columna
        """
        if nombre_columna not in self.columnas_problematicas:
            print(f"❌ La columna '{nombre_columna}' no tiene problemas identificados")
            return pd.DataFrame()

        indices_problematicos = self.columnas_problematicas[nombre_columna]['indices_problematicos']
        filas_problematicas = self.df_original.loc[indices_problematicos[:mostrar_filas]]

        print(f"\n🔍 FILAS PROBLEMÁTICAS DE LA COLUMNA '{nombre_columna}':")
        print("-" * 60)
        print(filas_problematicas.to_string())

        return filas_problematicas

    def crear_df_todas_las_filas_problematicas(self) -> pd.DataFrame:
        """
        Crea un DataFrame con todas las filas problemáticas para análisis general

        Returns:
            DataFrame con todas las filas problemáticas y información adicional
        """
        if not self.filas_problematicas:
            logger.info("No hay filas problemáticas para analizar")
            return pd.DataFrame()

        # Obtener todas las filas problemáticas únicas
        indices_unicos = sorted(list(set(self.filas_problematicas)))

        # Crear DataFrame con las filas problemáticas
        df_problemas = self.df_original.loc[indices_unicos].copy()

        # Agregar columna con el índice original
        df_problemas.insert(0, 'Indice_Original', indices_unicos)

        # Agregar columna indicando qué columnas tienen problemas en cada fila
        columnas_con_problemas = []
        for idx in indices_unicos:
            problemas_fila = []
            for columna, analisis in self.columnas_problematicas.items():
                if idx in analisis['indices_problematicos']:
                    problemas_fila.append(f"{columna}({analisis['tipo_detectado']})")
            columnas_con_problemas.append("; ".join(problemas_fila))

        df_problemas.insert(1, 'Columnas_Problematicas', columnas_con_problemas)

        # Calcular estadísticas por fila
        total_nulos_por_fila = []
        total_vacios_por_fila = []
        porcentaje_completitud_por_fila = []

        for idx in indices_unicos:
            fila = self.df_original.loc[idx]

            # Contar nulos y vacíos
            nulos = fila.isna().sum()
            vacios = (fila.astype(str) == '').sum()
            total_valores = len(fila)
            valores_validos = total_valores - nulos - vacios
            completitud = (valores_validos / total_valores) * 100

            total_nulos_por_fila.append(nulos)
            total_vacios_por_fila.append(vacios)
            porcentaje_completitud_por_fila.append(round(completitud, 2))

        df_problemas.insert(2, 'Total_Nulos', total_nulos_por_fila)
        df_problemas.insert(3, 'Total_Vacios', total_vacios_por_fila)
        df_problemas.insert(4, 'Porcentaje_Completitud', porcentaje_completitud_por_fila)

        logger.info(f"DataFrame de problemas creado con {len(df_problemas)} filas problemáticas")

        return df_problemas

    def exportar_reporte_columnas_problematicas(self, nombre_archivo: str = "reporte_columnas_problematicas.txt") -> str:
        """
        Exporta un reporte detallado de las columnas problemáticas a la carpeta Problems_to_solve

        Args:
            nombre_archivo: Nombre del archivo de reporte

        Returns:
            Ruta del archivo generado
        """
        ruta_reporte = os.path.join(self.directorio_problemas, nombre_archivo)

        with open(ruta_reporte, 'w', encoding='utf-8') as f:
            f.write("REPORTE DE ANÁLISIS DE COLUMNAS PROBLEMÁTICAS\n")
            f.write("=" * 60 + "\n")
            f.write(f"Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total columnas problemáticas: {len(self.columnas_problematicas)}\n\n")

            # Incluir información de carga del archivo
            if self.info_carga_archivo:
                f.write("INFORMACIÓN DE CARGA DEL ARCHIVO:\n")
                f.write("-" * 40 + "\n")
                f.write(f"Archivo: {self.info_carga_archivo['nombre_archivo']}\n")
                f.write(f"Tipo: {self.info_carga_archivo['tipo_archivo']}\n")
                f.write(f"Método de carga: {self.info_carga_archivo['metodo_carga']}\n")

                if 'fila_encabezados_detectada' in self.info_carga_archivo:
                    f.write(f"Fila de encabezados detectada: {self.info_carga_archivo['fila_encabezados_detectada']}\n")
                if 'fila_encabezados_final' in self.info_carga_archivo:
                    f.write(f"Fila de encabezados final: {self.info_carga_archivo['fila_encabezados_final']}\n")

                if self.info_carga_archivo['problemas_detectados']:
                    f.write("Problemas detectados en carga:\n")
                    for problema in self.info_carga_archivo['problemas_detectados']:
                        f.write(f"  - {problema}\n")
                f.write("\n")

            for columna, analisis in self.columnas_problematicas.items():
                f.write(f"\nCOLUMNA: {columna}\n")
                f.write("-" * 40 + "\n")
                f.write(f"Tipo: {analisis['tipo_detectado']}\n")
                f.write(f"Valores problemáticos: {analisis['total_valores_problematicos']}\n")
                f.write(f"Porcentaje: {analisis['porcentaje_problematicos']:.2f}%\n")

                f.write("\nEstadísticas:\n")
                stats = analisis['estadisticas_generales']
                for key, value in stats.items():
                    f.write(f"  {key}: {value}\n")

                f.write("\nValores problemáticos únicos:\n")
                for valor, count in analisis['valores_unicos_problematicos'].items():
                    f.write(f"  '{valor}': {count}\n")

                f.write("\nÍndices problemáticos (primeros 20):\n")
                indices = analisis['indices_problematicos'][:20]
                f.write(f"  {indices}\n")
                f.write("\n" + "="*60 + "\n")

        logger.info(f"📄 Reporte de texto exportado: {ruta_reporte}")
        return ruta_reporte

    def exportar_filas_problematicas_excel(self, nombre_archivo: str = "filas_problematicas_analisis.xlsx") -> str:
        """
        Exporta un archivo Excel con todas las filas problemáticas para análisis

        Args:
            nombre_archivo: Nombre del archivo Excel

        Returns:
            Ruta del archivo Excel generado
        """
        ruta_excel = os.path.join(self.directorio_problemas, nombre_archivo)

        try:
            # Crear DataFrame con todas las filas problemáticas
            df_problemas = self.crear_df_todas_las_filas_problematicas()

            if df_problemas.empty:
                logger.warning("No hay filas problemáticas para exportar")
                return ""

            # Crear el archivo Excel con múltiples hojas
            with pd.ExcelWriter(ruta_excel, engine='openpyxl') as writer:

                # Hoja 1: Resumen general
                df_resumen = pd.DataFrame({
                    'Métrica': [
                        'Total filas en archivo original',
                        'Total filas problemáticas',
                        'Porcentaje de filas problemáticas',
                        'Total columnas problemáticas',
                        'Fecha de análisis'
                    ],
                    'Valor': [
                        len(self.df_original),
                        len(df_problemas),
                        f"{(len(df_problemas) / len(self.df_original) * 100):.2f}%",
                        len(self.columnas_problematicas),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ]
                })
                df_resumen.to_excel(writer, sheet_name='Resumen', index=False)

                # Hoja 2: Todas las filas problemáticas
                df_problemas.to_excel(writer, sheet_name='Filas_Problematicas', index=False)

                # Hoja 3: Estadísticas por columna problemática
                estadisticas_columnas = []
                for columna, analisis in self.columnas_problematicas.items():
                    estadisticas_columnas.append({
                        'Columna': columna,
                        'Tipo_Detectado': analisis['tipo_detectado'],
                        'Total_Problemas': analisis['total_valores_problematicos'],
                        'Porcentaje_Problemas': f"{analisis['porcentaje_problematicos']:.2f}%",
                        'Total_Registros': analisis['estadisticas_generales']['total_registros'],
                        'Valores_Nulos': analisis['estadisticas_generales']['valores_nulos'],
                        'Valores_Vacios': analisis['estadisticas_generales']['valores_vacios'],
                        'Valores_Unicos': analisis['estadisticas_generales']['valores_unicos_totales']
                    })

                df_estadisticas = pd.DataFrame(estadisticas_columnas)
                df_estadisticas.to_excel(writer, sheet_name='Estadisticas_Columnas', index=False)

                # Hoja 4: Detalle por columna (solo primeras 5 columnas para evitar archivo muy grande)
                columnas_para_detalle = list(self.columnas_problematicas.keys())[:5]
                for i, columna in enumerate(columnas_para_detalle):
                    analisis = self.columnas_problematicas[columna]
                    indices_problematicos = analisis['indices_problematicos'][:100]  # Solo primeros 100

                    if indices_problematicos:
                        df_detalle = self.df_original.loc[indices_problematicos, [columna]].copy()
                        df_detalle.insert(0, 'Indice_Original', indices_problematicos)
                        df_detalle.insert(2, 'Tipo_Problema', analisis['tipo_detectado'])

                        # Nombre de hoja limitado
                        nombre_hoja = f"Detalle_{columna}"[:31]  # Excel limita a 31 caracteres
                        df_detalle.to_excel(writer, sheet_name=nombre_hoja, index=False)

            logger.info(f"📊 Archivo Excel de problemas exportado: {ruta_excel}")
            logger.info(f"   - Filas problemáticas: {len(df_problemas)}")
            logger.info(f"   - Columnas problemáticas: {len(self.columnas_problematicas)}")

            return ruta_excel

        except Exception as e:
            logger.error(f"Error al exportar Excel: {e}")
            return ""

    def exportar_filas_problematicas_formato_original(self, nombre_archivo: str = None) -> str:
        """
        🆕 NUEVO: Exporta las filas problemáticas en el mismo formato que el archivo original

        Args:
            nombre_archivo: Nombre personalizado (opcional)

        Returns:
            Ruta del archivo generado
        """
        try:
            # Crear DataFrame con filas problemáticas sin las columnas de análisis adicionales
            if not self.filas_problematicas:
                logger.warning("No hay filas problemáticas para exportar")
                return ""

            # Obtener todas las filas problemáticas únicas
            indices_unicos = sorted(list(set(self.filas_problematicas)))
            df_problemas_original = self.df_original.loc[indices_unicos].copy()

            # Determinar el formato del archivo original
            tipo_archivo_original = self.info_carga_archivo.get('tipo_archivo', 'Excel')
            nombre_archivo_original = self.info_carga_archivo.get('nombre_archivo', 'archivo_original')

            # Generar nombre del archivo si no se proporciona
            if nombre_archivo is None:
                nombre_base, extension_original = os.path.splitext(nombre_archivo_original)

                if tipo_archivo_original == 'CSV':
                    extension = '.csv'
                else:
                    extension = '.xlsx'

                nombre_archivo = f"{nombre_base}_filas_problematicas{extension}"

            # Ruta completa del archivo
            ruta_archivo = os.path.join(self.directorio_problemas, nombre_archivo)

            # Exportar según el formato original
            if tipo_archivo_original == 'CSV':
                # Exportar como CSV
                df_problemas_original.to_csv(ruta_archivo, index=True, encoding='utf-8')
                logger.info(f"📄 Filas problemáticas exportadas como CSV: {ruta_archivo}")

            else:
                # Exportar como Excel
                with pd.ExcelWriter(ruta_archivo, engine='openpyxl') as writer:
                    # Hoja principal con las filas problemáticas
                    df_problemas_original.to_excel(writer, sheet_name='Filas_Problematicas', index=True)

                    # Hoja adicional con información del análisis
                    info_analisis = []
                    for idx in indices_unicos:
                        problemas_fila = []
                        for columna, analisis in self.columnas_problematicas.items():
                            if idx in analisis['indices_problematicos']:
                                problemas_fila.append(f"{columna}({analisis['tipo_detectado']})")

                        info_analisis.append({
                            'Indice_Original': idx,
                            'Columnas_con_Problemas': "; ".join(problemas_fila),
                            'Cantidad_Problemas': len(problemas_fila)
                        })

                    df_info = pd.DataFrame(info_analisis)
                    df_info.to_excel(writer, sheet_name='Info_Problemas', index=False)

                logger.info(f"📊 Filas problemáticas exportadas como Excel: {ruta_archivo}")

            # Estadísticas del archivo generado
            logger.info(f"   - Total filas problemáticas: {len(df_problemas_original)}")
            logger.info(f"   - Total columnas: {len(df_problemas_original.columns)}")
            logger.info(f"   - Formato: {tipo_archivo_original}")
            logger.info(f"   - Índices incluidos: {indices_unicos[:10]}..." if len(indices_unicos) > 10 else f"   - Índices incluidos: {indices_unicos}")

            return ruta_archivo

        except Exception as e:
            logger.error(f"Error al exportar filas problemáticas en formato original: {e}")
            return ""

    def _guardar_archivo(self, nombre_archivo_original: str) -> str:
        """
        Guarda el DataFrame procesado

        Args:
            nombre_archivo_original: Nombre del archivo original

        Returns:
            Ruta del archivo guardado
        """
        nombre_base, extension = os.path.splitext(nombre_archivo_original)
        nuevo_nombre = f"{nombre_base}_processed{extension}"
        ruta_destino = os.path.join(self.directorio_destino, nuevo_nombre)

        try:
            if extension.lower() == '.csv':
                self.df.to_csv(ruta_destino, index=False, encoding='utf-8')
            elif extension.lower() in ['.xlsx', '.xls']:
                self.df.to_excel(ruta_destino, index=False, engine='openpyxl')

            logger.info(f"Archivo guardado exitosamente: {ruta_destino}")
            return ruta_destino

        except Exception as e:
            logger.error(f"Error al guardar archivo: {e}")
            raise

    def procesar_archivo(self, nombre_archivo: Optional[str] = None) -> Tuple[str, dict]:
        """
        Procesa un archivo específico o el primero disponible

        Args:
            nombre_archivo: Nombre específico del archivo a procesar

        Returns:
            Tupla con la ruta del archivo guardado y estadísticas del procesamiento
        """
        try:
            # Validar directorios
            self._validar_directorios()

            # Obtener archivos válidos
            archivos_validos = self._obtener_archivos_validos()

            # Seleccionar archivo a procesar
            if nombre_archivo:
                if nombre_archivo not in archivos_validos:
                    raise ValueError(f"Archivo '{nombre_archivo}' no encontrado o no válido")
                archivo_a_procesar = nombre_archivo
            else:
                archivo_a_procesar = archivos_validos[0]
                logger.info(f"Procesando primer archivo disponible: {archivo_a_procesar}")

            # Cargar y procesar datos
            self.df = self._cargar_archivo(archivo_a_procesar)
            dimensiones_originales = self.df.shape

            self._limpiar_datos()

            # Guardar archivo procesado
            ruta_guardado = self._guardar_archivo(archivo_a_procesar)

            # Estadísticas del procesamiento
            estadisticas = {
                'archivo_original': archivo_a_procesar,
                'archivo_procesado': ruta_guardado,
                'dimensiones_originales': dimensiones_originales,
                'dimensiones_finales': self.df.shape,
                'filas_problematicas': len(self.filas_problematicas),
                'columnas_procesadas': len(self.df.columns),
                'columnas_problematicas': len(self.columnas_problematicas),
                'info_carga': self.info_carga_archivo
            }

            return ruta_guardado, estadisticas

        except Exception as e:
            logger.error(f"Error durante el procesamiento: {e}")
            raise

    def procesar_todos_los_archivos(self) -> List[Tuple[str, dict]]:
        """
        Procesa todos los archivos válidos en el directorio origen

        Returns:
            Lista de tuplas con rutas y estadísticas de cada archivo procesado
        """
        resultados = []

        try:
            self._validar_directorios()
            archivos_validos = self._obtener_archivos_validos()

            logger.info(f"Procesando {len(archivos_validos)} archivos...")

            for archivo in archivos_validos:
                try:
                    ruta_guardado, estadisticas = self.procesar_archivo(archivo)
                    resultados.append((ruta_guardado, estadisticas))
                except Exception as e:
                    logger.error(f"Error procesando {archivo}: {e}")
                    continue

            logger.info(f"Procesamiento completado. {len(resultados)} archivos procesados exitosamente.")
            return resultados

        except Exception as e:
            logger.error(f"Error en procesamiento masivo: {e}")
            raise


def main():
    """Función principal para ejecutar el limpiador de datos"""
    try:
        # Crear instancia del limpiador
        limpiador = DataCleaner(
            directorio_origen="archive_original",
            directorio_destino="archive_processed"
        )

        # Procesar archivo (toma el primero disponible)
        ruta_guardado, estadisticas = limpiador.procesar_archivo()

        # Mostrar información de carga del archivo
        limpiador.imprimir_info_carga_archivo()

        # Mostrar resultados
        print("\n" + "="*60)
        print("PROCESAMIENTO COMPLETADO")
        print("="*60)
        print(f"Archivo original: {estadisticas['archivo_original']}")
        print(f"Archivo procesado: {estadisticas['archivo_procesado']}")
        print(f"Dimensiones originales: {estadisticas['dimensiones_originales']}")
        print(f"Dimensiones finales: {estadisticas['dimensiones_finales']}")
        print(f"Filas con problemas identificadas: {estadisticas['filas_problematicas']}")
        print(f"Columnas procesadas: {estadisticas['columnas_procesadas']}")
        print(f"Columnas problemáticas: {estadisticas['columnas_problematicas']}")

        # ¡NUEVA FUNCIONALIDAD! - Mostrar análisis de columnas problemáticas
        if estadisticas['columnas_problematicas'] > 0:
            print("\n" + "="*60)
            print("ANÁLISIS DETALLADO DE PROBLEMAS")
            print("="*60)

            # Imprimir análisis completo
            limpiador.imprimir_analisis_columnas_problematicas(mostrar_detalle=True)

            # Exportar reporte de texto
            ruta_reporte = limpiador.exportar_reporte_columnas_problematicas()
            print(f"\n📄 Reporte detallado exportado: {ruta_reporte}")

            # Exportar archivo Excel con todas las filas problemáticas
            ruta_excel = limpiador.exportar_filas_problematicas_excel()
            if ruta_excel:
                print(f"📊 Archivo Excel con filas problemáticas: {ruta_excel}")

            # 🆕 NUEVO: Exportar filas problemáticas en formato original
            ruta_formato_original = limpiador.exportar_filas_problematicas_formato_original()
            if ruta_formato_original:
                tipo_archivo = limpiador.info_carga_archivo.get('tipo_archivo', 'Excel')
                print(f"📋 Filas problemáticas en formato {tipo_archivo}: {ruta_formato_original}")
                
                # Mostrar resumen del DataFrame de problemas
                df_problemas = limpiador.crear_df_todas_las_filas_problematicas()
                if not df_problemas.empty:
                    print(f"\n🔍 RESUMEN DE ANÁLISIS GENERAL:")
                    print(f"   - Total filas problemáticas: {len(df_problemas)}")
                    print(f"   - Columnas en análisis: {list(df_problemas.columns)}")
                    print(f"   - Completitud promedio: {df_problemas['Porcentaje_Completitud'].mean():.2f}%")
                    
                    # Mostrar distribución de problemas por tipo
                    print(f"\n📈 DISTRIBUCIÓN DE PROBLEMAS:")
                    for columna, analisis in limpiador.columnas_problematicas.items():
                        print(f"   - {columna} ({analisis['tipo_detectado']}): {analisis['total_valores_problematicos']} problemas")
            
            # Ejemplo de análisis específico por columna
            if limpiador.columnas_problematicas:
                primera_columna = list(limpiador.columnas_problematicas.keys())[0]
                print(f"\n🔍 EJEMPLO DE ANÁLISIS ESPECÍFICO:")
                limpiador.obtener_filas_problematicas_por_columna(primera_columna, 5)
        
        print("="*60)
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())