# app/utils/queries.py

def get_sales_query(category: str = None) -> str:
    """
    Construye la consulta SQL para obtener datos de ventas, con un filtro de categoría opcional.
    """
    base_query = "SELECT * FROM workspace.tecnomundo_data_gold.fact_sales"
    if category:
        base_query += f" WHERE categoria = '{category}'"
    return base_query


def get_categories_query() -> str:
    """
    Construye la consulta SQL para obtener la lista de categorías únicas.
    """
    return "SELECT DISTINCT categoria FROM workspace.tecnomundo_data_gold.dim_products WHERE categoria IS NOT NULL ORDER BY categoria"


def get_inventory_query(category: str = None) -> str:
    """
    Construye la consulta SQL para el análisis de inventario.
    Calcula las ventas de los últimos 30 días para cada producto basándose en su fecha de venta más reciente.
    """
    query = f"""
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
        query += f" AND ls.categoria = '{category}'"

    return query