import os
import pandas as pd
from databricks import sql
import logging
# Importamos las nuevas funciones de queries
from app.utils.queries import (
    get_inventory_query,
    get_sales_query,
    get_categories_query,
    get_sales_date_range_query,
    get_sales_trend_query
)

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
            raise

    def execute_query(self, query: str, params: list = None) -> pd.DataFrame:
        """
        Ejecuta una consulta SQL de forma segura, utilizando parámetros.
        """
        if not self.connection:
            logger.error("No hay conexión a Databricks para ejecutar la consulta.")
            return pd.DataFrame()
        try:
            with self.connection.cursor() as cursor:
                logger.debug(f"Ejecutando consulta: {query} con parámetros: {params}")
                cursor.execute(query, params or [])
                result = cursor.fetchall_arrow().to_pandas()
                return result
        except Exception as e:
            logger.error(f"Error al ejecutar la consulta: {e}")
            return pd.DataFrame()

    def get_inventory_data(self, category: str = None) -> pd.DataFrame:
        query, params = get_inventory_query(category)
        return self.execute_query(query, params)

    def get_sales_data(self, category: str = None) -> pd.DataFrame:
        query, params = get_sales_query(category)
        return self.execute_query(query, params)

    def get_categories(self) -> list:
        query = get_categories_query()
        df = self.execute_query(query)
        if not df.empty:
            return df['categoria'].tolist()
        return []

    # --- NUEVAS FUNCIONES DEL CONECTOR ---
    def get_sales_date_range(self) -> pd.DataFrame:
        """
        Obtiene el rango de fechas de ventas.
        """
        query = get_sales_date_range_query()
        return self.execute_query(query)

    def get_sales_trend_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Obtiene los datos de tendencia de ventas para un rango.
        """
        query, params = get_sales_trend_query(start_date, end_date)
        return self.execute_query(query, params)

    def close_connection(self):
        """
        Cierra la conexión a Databricks si está abierta.
        """
        if self.connection:
            self.connection.close()
            logger.info("Conexión a Databricks cerrada.")