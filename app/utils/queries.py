# app/utils/queries.py
from typing import Tuple, List, Any

def get_sales_query(category: str = None) -> Tuple[str, List[Any]]:
    """
    Construye la consulta SQL para ventas, usando '?' como marcador de posición
    para compatibilidad con el conector de Databricks.
    """
    base_query = "SELECT * FROM workspace.tecnomundo_data_gold.fact_sales"
    params = []
    if category:
        # CAMBIO: Usamos '?' en lugar de '%s'
        base_query += " WHERE categoria = ?"
        params.append(category)
    return base_query, params


def get_categories_query() -> str:
    """
    Construye la consulta SQL para obtener la lista de categorías únicas.
    """
    return "SELECT DISTINCT categoria FROM workspace.tecnomundo_data_gold.dim_products WHERE categoria IS NOT NULL ORDER BY categoria"


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
            codigo_producto,
            nombre_del_producto,
            categoria,
            stock_actual,
            ROW_NUMBER() OVER(PARTITION BY codigo_producto ORDER BY fecha DESC) as rn
        FROM workspace.tecnomundo_data_gold.fact_sales
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
    WHERE ls.rn = 1
    """

    if category:
        # CAMBIO: Usamos '?' en lugar de '%s'
        query += " AND ls.categoria = ?"
        params.append(category)

    return query, params
