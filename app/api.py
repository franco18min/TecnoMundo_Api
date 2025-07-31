# app/api.py

from flask import Blueprint, render_template, jsonify, request
from app.services.dashboard_service import DashboardService
from .extensions import cache  # Importamos 'cache' desde el nuevo archivo extensions.py
import logging

api_bp = Blueprint('api', __name__, template_folder='templates', static_folder='static')
dashboard_service = DashboardService()
logger = logging.getLogger(__name__)

@api_bp.route('/')
def index():
    """
    Endpoint de la API que renderiza la página principal del dashboard.
    """
    return render_template('dashboard.html')

@api_bp.route('/data/sales_analysis')
@cache.cached(timeout=300, query_string=True)
def get_sales_analysis_data():
    """
    Endpoint para el análisis de ventas.
    La respuesta se cachea por 300 segundos (5 minutos).
    'query_string=True' crea un caché diferente para cada combinación de parámetros.
    """
    logger.info("¡Endpoint de análisis de ventas ejecutado! No se encontró caché para esta petición.")
    try:
        category = request.args.get('category')
        top_n = request.args.get('top_n', 10, type=int)
        if category and category.lower() == 'all':
            category = None
        data = dashboard_service.get_sales_analysis_data(category, top_n)
        return jsonify(data) if data else (jsonify({"error": "No se pudieron obtener los datos"}), 500)
    except Exception as e:
        logger.error(f"Error en el endpoint /data/sales_analysis: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500

@api_bp.route('/categories')
@cache.cached(timeout=3600)
def get_categories():
    """
    Endpoint para obtener la lista de categorías. Se cachea por 1 hora.
    """
    logger.info("¡Endpoint de categorías ejecutado! No se encontró caché.")
    try:
        categories = dashboard_service.get_all_categories()
        return jsonify(categories)
    except Exception as e:
        logger.error(f"Error en el endpoint /categories: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500

@api_bp.route('/data/inventory_analysis')
@cache.cached(timeout=300, query_string=True)
def get_inventory_analysis_data():
    """
    Endpoint para obtener los datos de la sección de inteligencia de inventario.
    """
    logger.info("¡Endpoint de análisis de inventario ejecutado! No se encontró caché para esta petición.")
    try:
        category = request.args.get('category')
        if category and category.lower() == 'all':
            category = None

        data = dashboard_service.get_inventory_analysis_data(category)

        if data is not None:
            return jsonify(data)
        else:
            return jsonify({"error": "No se pudieron obtener los datos de inventario"}), 500

    except Exception as e:
        logger.error(f"Error en el endpoint /data/inventory_analysis: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500

@api_bp.route('/reports/inventory_health')
@cache.cached(timeout=3600) # Cachear el informe por 1 hora
def get_inventory_health_report():
    """
    Endpoint para obtener los datos consolidados para el informe de salud de inventario.
    """
    try:
        # Llama al nuevo método en el servicio
        report_data = dashboard_service.get_inventory_health_report_data()
        if report_data:
            return jsonify(report_data)
        else:
            return jsonify({"error": "No se pudo generar el informe"}), 500
    except Exception as e:
        logger.error(f"Error en el endpoint /reports/inventory_health: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500