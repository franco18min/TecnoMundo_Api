# app/utils/queries.py
from typing import Tuple, List, Any

def get_sales_query(category: str = None) -> Tuple[str, List[Any]]:
    """
    Construye la consulta SQL para ventas, uniendo con la tabla de productos
    para poder filtrar por categoría y obtener el nombre del producto.
    """
    base_query = """
    SELECT
        s.codigo_producto,
        p.nombre_del_producto,
        p.categoria,
        s.cantidad,
        s.precio_unitario,
        s.fecha
    FROM workspace.tecnomundo_data_gold.fact_sales s
    LEFT JOIN workspace.tecnomundo_data_gold.dim_products p ON s.codigo_producto = p.codigo_producto
    """
    params = []
    # Se añade un WHERE para filtrar por categoría si es necesario, y siempre se excluye 'Servicio Tecnico' de este análisis.
    if category and category.lower() != 'all':
        base_query += " WHERE p.categoria = ? AND p.categoria != 'Servicio Tecnico'"
        params.append(category)
    else:
        base_query += " WHERE p.categoria != 'Servicio Tecnico' OR p.categoria IS NULL"

    return base_query, params


def get_categories_query() -> str:
    """
    Construye la consulta SQL para obtener la lista de categorías únicas.
    """
    return "SELECT DISTINCT categoria FROM workspace.tecnomundo_data_gold.dim_products WHERE categoria IS NOT NULL AND categoria != 'Servicio Tecnico' ORDER BY categoria"


def get_inventory_query(category: str = None) -> Tuple[str, List[Any]]:
    """
    Construye la consulta SQL para el análisis de inventario.
    Usa '?' como marcador de posición para los parámetros.
    """
    params = []
    query = """
    WITH ProductMaxDate AS (
        SELECT
            codigo_producto,
            MAX(CAST(fecha AS DATE)) as max_fecha_producto
        FROM workspace.tecnomundo_data_gold.fact_sales
        GROUP BY codigo_producto
    ),
    LatestStock AS (
        SELECT
            s.codigo_producto,
            p.nombre_del_producto,
            p.categoria,
            s.stock_actual,
            ROW_NUMBER() OVER(PARTITION BY s.codigo_producto ORDER BY s.fecha DESC) as rn
        FROM workspace.tecnomundo_data_gold.fact_sales s
        JOIN workspace.tecnomundo_data_gold.dim_products p ON s.codigo_producto = p.codigo_producto
    ),
    SalesLast30Days AS (
        SELECT
            s.codigo_producto,
            SUM(s.cantidad) as unidades_vendidas_30d
        FROM workspace.tecnomundo_data_gold.fact_sales s
        JOIN ProductMaxDate pmd ON s.codigo_producto = pmd.codigo_producto
        WHERE CAST(s.fecha AS DATE) >= date_sub(pmd.max_fecha_producto, 30)
        GROUP BY s.codigo_producto
    )
    SELECT
        ls.codigo_producto,
        ls.nombre_del_producto,
        ls.categoria,
        ls.stock_actual,
        COALESCE(s30.unidades_vendidas_30d, 0) as unidades_vendidas_30d
    FROM LatestStock ls
    LEFT JOIN SalesLast30Days s30 ON ls.codigo_producto = s30.codigo_producto
    WHERE ls.rn = 1 AND ls.categoria != 'Servicio Tecnico'
    """

    if category:
        query += " AND ls.categoria = ?"
        params.append(category)

    return query, params


def get_sales_date_range_query() -> str:
    """
    Obtiene la fecha mínima y máxima de todas las ventas de productos,
    excluyendo la categoría 'Servicio Tecnico'.
    """
    return """
    SELECT
        MIN(CAST(s.fecha AS DATE)) as min_date,
        MAX(CAST(s.fecha AS DATE)) as max_date
    FROM workspace.tecnomundo_data_gold.fact_sales s
    LEFT JOIN workspace.tecnomundo_data_gold.dim_products p ON s.codigo_producto = p.codigo_producto
    WHERE p.categoria != 'Servicio Tecnico' OR p.categoria IS NULL
    """


def get_sales_trend_query(start_date: str, end_date: str) -> Tuple[str, List[Any]]:
    """
    Obtiene la tendencia de ventas agregada por día para un rango de fechas,
    excluyendo la categoría 'Servicio Tecnico'.
    """
    query = """
    SELECT
        CAST(s.fecha AS DATE) as fecha_venta,
        SUM(s.cantidad) as total_unidades
    FROM workspace.tecnomundo_data_gold.fact_sales s
    LEFT JOIN workspace.tecnomundo_data_gold.dim_products p ON s.codigo_producto = p.codigo_producto
    WHERE (CAST(s.fecha AS DATE) BETWEEN ? AND ?)
    AND (p.categoria != 'Servicio Tecnico' OR p.categoria IS NULL)
    GROUP BY CAST(s.fecha AS DATE)
    ORDER BY fecha_venta ASC
    """
    params = [start_date, end_date]
    return query, params
