import pandas as pd
from app.utils.databricks_connector import DatabricksConnector
import logging

logger = logging.getLogger(__name__)


class DashboardService:
    def __init__(self):
        self.connector = DatabricksConnector()

    def get_sales_analysis_data(self, category: str = None, top_n: int = 10):
        """
        Prepara los datos para la sección de análisis de ventas.
        Calcula el top N de productos más vendidos por cantidad.
        """
        try:
            df_sales = self.connector.get_sales_data(category)

            if df_sales.empty:
                return {"top_products_by_quantity": []}

            # Usar el parámetro top_n para que sea dinámico
            top_products = df_sales.groupby(['codigo_producto', 'nombre_del_producto'])['cantidad'].sum().nlargest(
                top_n).reset_index()
            top_products.rename(columns={'cantidad': 'total_unidades'}, inplace=True)

            return {
                "top_products_by_quantity": top_products.to_dict(orient='records')
            }
        except Exception as e:
            logger.error(f"Error al procesar los datos de análisis de ventas: {e}")
            return None

    def get_all_categories(self):
        """
        Obtiene todas las categorías de productos.
        """
        try:
            return self.connector.get_categories()
        except Exception as e:
            logger.error(f"Error al obtener las categorías: {e}")
            return []
