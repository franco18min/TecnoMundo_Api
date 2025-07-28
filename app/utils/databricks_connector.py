import os
from databricks import sql
import pandas as pd
import logging
# Importamos las funciones que construyen las consultas
from app.utils.queries import get_sales_query, get_categories_query, get_inventory_query

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabricksConnector:
    def __init__(self):
        self.server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.access_token = os.getenv("DATABRICKS_TOKEN")

        if not all([self.server_hostname, self.http_path, self.access_token]):
            raise ValueError("Las variables de entorno de Databricks no están configuradas correctamente.")

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Método genérico para ejecutar una consulta y devolver un DataFrame de Pandas.
        """
        logger.info(f"Ejecutando consulta: {query[:200]}...")  # Loguea los primeros 200 caracteres
        try:
            with sql.connect(
                    server_hostname=self.server_hostname,
                    http_path=self.http_path,
                    access_token=self.access_token
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    return pd.DataFrame(result, columns=columns) if result else pd.DataFrame()
        except Exception as e:
            logger.error(f"Error al ejecutar la consulta en Databricks: {e}")
            raise

    def get_sales_data(self, category: str = None) -> pd.DataFrame:
        query = get_sales_query(category)
        return self.execute_query(query)

    def get_categories(self) -> list:
        query = get_categories_query()
        df = self.execute_query(query)
        return df.iloc[:, 0].tolist() if not df.empty else []

    def get_inventory_data(self, category: str = None) -> pd.DataFrame:
        query = get_inventory_query(category)
        return self.execute_query(query)
