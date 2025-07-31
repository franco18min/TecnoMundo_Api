# app/utils/databricks_connector.py

import os
import pandas as pd
from databricks import sql
import logging
from app.utils.queries import get_inventory_query, get_sales_query, get_categories_query

# Configuración del logger para este módulo
logger = logging.getLogger(__name__)

class DatabricksConnector:
    """
    Clase para manejar la conexión y ejecución de consultas en Databricks.
    """
    def __init__(self):
        """
        Inicializa el conector y establece la conexión al instanciar la clase.
        """
        self.connection = None
        try:
            self.connection = sql.connect(
                server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                http_path=os.getenv("DATABRICKS_HTTP_PATH"),
                access_token=os.getenv("DATABRICKS_TOKEN")
            )
            logger.info("Conexión a Databricks establecida exitosamente.")
        except Exception as e:
            logger.error(f"No se pudo conectar a Databricks: {e}")
            # Levantar la excepción para que el servicio que lo llama sepa que falló
            raise

    def execute_query(self, query: str, params: list = None) -> pd.DataFrame:
        """
        Ejecuta una consulta SQL de forma segura, utilizando parámetros para prevenir inyección SQL.

        Args:
            query (str): La cadena de la consulta SQL con marcadores de posición (%s).
            params (list, optional): Una lista de parámetros para sustituir en la consulta. Defaults to None.

        Returns:
            pd.DataFrame: Un DataFrame de pandas con los resultados, o un DataFrame vacío si no hay resultados.
        """
        if not self.connection:
            logger.error("No hay conexión a Databricks para ejecutar la consulta.")
            return pd.DataFrame()

        try:
            with self.connection.cursor() as cursor:
                # ¡AQUÍ ESTÁ EL CAMBIO CLAVE!
                # Pasamos los parámetros de forma segura como un segundo argumento a execute().
                # El conector se encarga de escapar cualquier carácter especial (como '/').
                logger.debug(f"Ejecutando consulta: {query} con parámetros: {params}")
                cursor.execute(query, params or [])
                result = cursor.fetchall_arrow().to_pandas()
                return result
        except Exception as e:
            logger.error(f"Error al ejecutar la consulta: {e}")
            return pd.DataFrame() # Devolver un DataFrame vacío en caso de error

    def get_inventory_data(self, category: str = None) -> pd.DataFrame:
        """
        Obtiene los datos de inventario llamando a la función de consulta correspondiente.
        """
        # Obtenemos la consulta y los parámetros desde queries.py
        query, params = get_inventory_query(category)
        # Ejecutamos la consulta de forma segura
        return self.execute_query(query, params)

    def get_sales_data(self, category: str = None) -> pd.DataFrame:
        """
        Obtiene los datos de ventas llamando a la función de consulta correspondiente.
        """
        query, params = get_sales_query(category)
        return self.execute_query(query, params)

    def get_categories(self) -> list:
        """
        Obtiene la lista de categorías únicas. Esta consulta no necesita parámetros.
        """
        query = get_categories_query()
        df = self.execute_query(query)
        if not df.empty:
            return df['categoria'].tolist()
        return []

    def close_connection(self):
        """
        Cierra la conexión a Databricks si está abierta.
        """
        if self.connection:
            self.connection.close()
            logger.info("Conexión a Databricks cerrada.")

