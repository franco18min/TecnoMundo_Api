from flask import Blueprint, render_template, jsonify, request
from app.services.dashboard_service import DashboardService
import logging

api_bp = Blueprint('api', __name__, template_folder='templates', static_folder='static')
dashboard_service = DashboardService()
logger = logging.getLogger(__name__)


@api_bp.route('/')
def index():
    return render_template('dashboard.html')


@api_bp.route('/data/sales_analysis')
def get_sales_analysis_data():
    # ... (código existente sin cambios)
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
def get_categories():
    # ... (código existente sin cambios)
    try:
        categories = dashboard_service.get_all_categories()
        return jsonify(categories)
    except Exception as e:
        logger.error(f"Error en el endpoint /categories: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500


# --- NUEVO ENDPOINT ---
@api_bp.route('/data/inventory_analysis')
def get_inventory_analysis_data():
    """
    Endpoint para obtener los datos de la sección de inteligencia de inventario.
    """
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
