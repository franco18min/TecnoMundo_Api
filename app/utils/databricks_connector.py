import os
from databricks import sql
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabricksConnector:
    def __init__(self):
        self.server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.access_token = os.getenv("DATABRICKS_TOKEN")

        if not all([self.server_hostname, self.http_path, self.access_token]):
            raise ValueError("Las variables de entorno de Databricks no están configuradas correctamente.")

    def _execute_query(self, query: str):
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
                    return result, columns
        except Exception as e:
            logger.error(f"Error al ejecutar la consulta en Databricks: {e}")
            raise

    def get_sales_data(self, category: str = None) -> pd.DataFrame:
        base_query = "SELECT * FROM workspace.tecnomundo_data_gold.fact_sales"

        conditions = []
        if category:
            conditions.append(f"categoria = '{category}'")

        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)

        logger.info(f"Ejecutando consulta de ventas: {base_query}")

        result, columns = self._execute_query(base_query)
        if result:
            return pd.DataFrame(result, columns=columns)
        return pd.DataFrame()

    def get_categories(self) -> list:
        query = "SELECT DISTINCT categoria FROM workspace.tecnomundo_data_gold.dim_products WHERE categoria IS NOT NULL ORDER BY categoria"
        logger.info(f"Ejecutando consulta de categorías: {query}")

        result, _ = self._execute_query(query)
        if result:
            return [row[0] for row in result]
        return []
