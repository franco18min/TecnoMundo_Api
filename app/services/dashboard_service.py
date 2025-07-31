import pandas as pd
import numpy as np
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

            # --- 1. Limpieza de Datos ---
            # Nos aseguramos de que las columnas numéricas no contengan valores nulos (NULL/NaN).
            df['stock_actual'] = pd.to_numeric(df['stock_actual']).fillna(0)
            df['unidades_vendidas_30d'] = pd.to_numeric(df['unidades_vendidas_30d']).fillna(0)

            # --- 2. Cálculo de Días de Inventario (DOI) ---
            # Se calcula la venta diaria promedio.
            venta_diaria_promedio = df['unidades_vendidas_30d'] / 30

            # Se calcula el DOI. np.where previene la división por cero si no hay ventas.
            # Si no hay ventas, los días de inventario son infinitos (np.inf).
            df['dias_inventario'] = np.where(
                venta_diaria_promedio > 0,
                df['stock_actual'] / venta_diaria_promedio,
                np.inf
            )

            # --- 3. Nueva Lógica de Estado basada en DOI ---
            def assign_status_doi(row):
                stock = row['stock_actual']
                ventas_30d = row['unidades_vendidas_30d']
                doi = row['dias_inventario']

                if stock <= 0:
                    return 'Sin Stock'
                if ventas_30d == 0:
                    return 'Inventario Estancado'
                if doi <= 7:  # Menos de 1 semana de stock
                    return 'Riesgo de Quiebre'
                if doi <= 30:  # Entre 1 semana y 1 mes
                    return 'Alta Rotación'
                if doi > 90:  # Más de 3 meses de stock
                    return 'Lenta Rotación'
                return 'Rotación Saludable'

            df['estado'] = df.apply(assign_status_doi, axis=1)

            # 4. Seleccionamos las columnas finales para la respuesta a la API.
            df_final = df[['nombre_del_producto', 'stock_actual', 'unidades_vendidas_30d', 'estado']].copy()

            return df_final.to_dict(orient='records')

        except Exception as e:
            logger.error(f"Error al procesar los datos de inventario: {e}")
            return None

    def get_inventory_analysis_data(self, category: str = None):
        # ... (este método se mantiene exactamente igual)
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

            df_final = df[['nombre_del_producto', 'stock_actual', 'unidades_vendidas_30d', 'estado']].copy()

            return df_final.to_dict(orient='records')

        except Exception as e:
            logger.error(f"Error al procesar los datos de inventario: {e}")
            return None

    def get_inventory_health_report_data(self):
        """
        Calcula los KPIs y la distribución de estados para el informe de salud de inventario.
        """
        try:
            # Reutilizamos la función que ya tenemos para obtener todos los datos sin filtro de categoría.
            inventory_list = self.get_inventory_analysis_data()
            if not inventory_list:
                return {
                    "kpis": {"risk_products_count": 0, "stagnant_products_count": 0, "healthy_percentage": 0},
                    "distribution": {}
                }

            df = pd.DataFrame(inventory_list)

            # Calcular KPIs
            total_products = len(df)
            risk_products_count = len(df[df['estado'] == 'Riesgo de Quiebre'])
            stagnant_products_count = len(df[df['estado'] == 'Inventario Estancado'])
            healthy_states = ['Rotación Saludable', 'Alta Rotación']
            healthy_products_count = len(df[df['estado'].isin(healthy_states)])
            healthy_percentage = (healthy_products_count / total_products) * 100 if total_products > 0 else 0

            # Calcular distribución para el gráfico
            distribution = df['estado'].value_counts().to_dict()

            return {
                "kpis": {
                    "risk_products_count": int(risk_products_count),
                    "stagnant_products_count": int(stagnant_products_count),
                    "healthy_percentage": healthy_percentage
                },
                "distribution": distribution
            }
        except Exception as e:
            logger.error(f"Error al generar el informe de salud de inventario: {e}")
            return None