import pandas as pd
from app.utils.databricks_connector import DatabricksConnector
import logging

logger = logging.getLogger(__name__)


class DashboardService:
    def __init__(self):
        self.connector = DatabricksConnector()

    def get_sales_analysis_data(self, category: str = None, top_n: int = 10):
        try:
            df_sales = self.connector.get_sales_data(category)
            if df_sales.empty:
                return {"top_products_by_quantity": []}
            top_products = df_sales.groupby(['codigo_producto', 'nombre_del_producto'])['cantidad'].sum().nlargest(
                top_n).reset_index()
            top_products.rename(columns={'cantidad': 'total_unidades'}, inplace=True)
            return {"top_products_by_quantity": top_products.to_dict(orient='records')}
        except Exception as e:
            logger.error(f"Error al procesar los datos de análisis de ventas: {e}")
            return None

    def get_all_categories(self):
        try:
            return self.connector.get_categories()
        except Exception as e:
            logger.error(f"Error al obtener las categorías: {e}")
            return []

    def get_inventory_analysis_data(self, category: str = None):
        try:
            df = self.connector.get_inventory_data(category)
            if df.empty:
                return []

            # Lógica de Estado basada en el movimiento de ventas
            def assign_status(row):
                ventas_30d = row['unidades_vendidas_30d']
                stock = row['stock_actual']

                if stock <= 0:
                    return 'Sin Stock'
                if ventas_30d == 0:
                    return 'Inventario Estancado'
                if ventas_30d > 20 and stock < 10:  # Se vende mucho, queda poco
                    return 'Riesgo de Quiebre'
                if ventas_30d > 10:  # Se vende bien
                    return 'Alta Rotación'
                if ventas_30d < 5 and stock > 20:  # Se vende poco, hay mucho
                    return 'Lenta Rotación'
                return 'Rotación Saludable'

            df['estado'] = df.apply(assign_status, axis=1)

            # CORRECCIÓN: Se elimina 'dias_inventario' de la selección final
            df_final = df[['nombre_del_producto', 'stock_actual', 'unidades_vendidas_30d', 'estado']].copy()

            return df_final.to_dict(orient='records')

        except Exception as e:
            logger.error(f"Error al procesar los datos de inventario: {e}")
            return None
