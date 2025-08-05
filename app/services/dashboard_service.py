# app/services/dashboard_service.py
import pandas as pd
import numpy as np
from app.utils.databricks_connector import DatabricksConnector
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DashboardService:
    def __init__(self):
        self.connector = DatabricksConnector()

    def get_sales_analysis_data(self, category: str = None, top_n: int = 10):
        """
        Obtiene los datos para el análisis de ventas.
        LÓGICA: Obtiene ventas y productos por separado y los une en pandas
        para mayor robustez contra errores de JOIN en la base de datos.
        """
        try:
            logger.info("Obteniendo datos de ventas y productos por separado...")
            sales_query = "SELECT codigo_producto, cantidad FROM workspace.tecnomundo_data_gold.fact_sales"
            products_query = "SELECT codigo_producto, nombre_del_producto, categoria FROM workspace.tecnomundo_data_gold.dim_products"

            df_sales = self.connector.execute_query(sales_query)
            df_products = self.connector.execute_query(products_query)

            if df_sales.empty or df_products.empty:
                logger.warning("Una de las tablas (ventas o productos) está vacía.")
                return {"top_products_by_quantity": []}

            df_sales['codigo_producto'] = df_sales['codigo_producto'].astype(str)
            df_products['codigo_producto'] = df_products['codigo_producto'].astype(str)

            df_merged = pd.merge(df_sales, df_products, on='codigo_producto', how='left')

            if category and category.lower() != 'all':
                df_filtered = df_merged[df_merged['categoria'] == category].copy()
            else:
                df_filtered = df_merged.copy()

            df_filtered.dropna(subset=['nombre_del_producto'], inplace=True)
            df_filtered['cantidad'] = pd.to_numeric(df_filtered['cantidad'], errors='coerce').fillna(0)

            if df_filtered.empty:
                logger.warning("El DataFrame está vacío después de filtrar y limpiar.")
                return {"top_products_by_quantity": []}

            top_products = df_filtered.groupby(['codigo_producto', 'nombre_del_producto'])['cantidad'].sum().nlargest(
                top_n).reset_index()
            top_products.rename(columns={'cantidad': 'total_unidades'}, inplace=True)

            return {"top_products_by_quantity": top_products.to_dict(orient='records')}

        except Exception as e:
            logger.error(f"Error crítico al procesar los datos de análisis de ventas: {e}", exc_info=True)
            return {"top_products_by_quantity": []}

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

            df['stock_actual'] = pd.to_numeric(df['stock_actual']).fillna(0)
            df['unidades_vendidas_30d'] = pd.to_numeric(df['unidades_vendidas_30d']).fillna(0)

            venta_diaria_promedio = df['unidades_vendidas_30d'] / 30
            df['dias_inventario'] = np.where(
                venta_diaria_promedio > 0,
                df['stock_actual'] / venta_diaria_promedio,
                np.inf
            )

            def assign_status_doi(row):
                stock = row['stock_actual']
                ventas_30d = row['unidades_vendidas_30d']
                doi = row['dias_inventario']

                if stock <= 0:
                    return 'Sin Stock'
                if ventas_30d == 0:
                    return 'Inventario Estancado'
                if doi <= 7:
                    return 'Riesgo de Quiebre'
                if doi <= 30:
                    return 'Alta Rotación'
                if doi > 90:
                    return 'Lenta Rotación'
                return 'Rotación Saludable'

            df['estado'] = df.apply(assign_status_doi, axis=1)

            df_final = df[
                ['nombre_del_producto', 'stock_actual', 'unidades_vendidas_30d', 'estado', 'categoria']].copy()
            return df_final.to_dict(orient='records')

        except Exception as e:
            logger.error(f"Error al procesar los datos de inventario: {e}")
            return None

    def get_inventory_health_report_data(self):
        try:
            inventory_list = self.get_inventory_analysis_data()

            if not inventory_list:
                return {
                    "kpis": {"risk_products_count": 0, "stagnant_products_count": 0, "healthy_percentage": 0},
                    "distribution": {},
                    "inventory_data": []
                }

            df = pd.DataFrame(inventory_list)
            total_products = len(df)
            risk_products_count = len(df[df['estado'] == 'Riesgo de Quiebre'])
            stagnant_products_count = len(df[df['estado'] == 'Inventario Estancado'])
            healthy_states = ['Rotación Saludable', 'Alta Rotación']
            healthy_products_count = len(df[df['estado'].isin(healthy_states)])
            healthy_percentage = (healthy_products_count / total_products) * 100 if total_products > 0 else 0
            distribution = df['estado'].value_counts().to_dict()

            return {
                "kpis": {
                    "risk_products_count": int(risk_products_count),
                    "stagnant_products_count": int(stagnant_products_count),
                    "healthy_percentage": healthy_percentage
                },
                "distribution": distribution,
                "inventory_data": inventory_list
            }
        except Exception as e:
            logger.error(f"Error al generar el informe de salud de inventario: {e}")
            return None

    def get_sales_date_range(self):
        try:
            df = self.connector.get_sales_date_range()
            if not df.empty and df['min_date'][0] is not None:
                min_date = pd.to_datetime(df['min_date'][0]).strftime('%Y-%m-%d')
                max_date = pd.to_datetime(df['max_date'][0]).strftime('%Y-%m-%d')
                return {"min_date": min_date, "max_date": max_date}
            return None
        except Exception as e:
            logger.error(f"Error al obtener el rango de fechas de ventas: {e}")
            return None

    def get_sales_trend(self, start_date: str, end_date: str):
        try:
            df = self.connector.get_sales_trend_data(start_date, end_date)

            date_range_index = pd.date_range(start=start_date, end=end_date, freq='D')

            if df.empty:
                df = pd.DataFrame(0, index=date_range_index, columns=['unidades']).reset_index()
                df.rename(columns={'index': 'fecha'}, inplace=True)
            else:
                df['fecha_venta'] = pd.to_datetime(df['fecha_venta'])
                df.set_index('fecha_venta', inplace=True)
                df = df.reindex(date_range_index, fill_value=0).reset_index()
                df.rename(columns={'index': 'fecha', 'total_unidades': 'unidades'}, inplace=True)

            df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')

            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"Error al obtener la tendencia de ventas: {e}")
            return None
