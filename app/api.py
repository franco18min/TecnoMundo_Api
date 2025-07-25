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
    try:
        category = request.args.get('category')
        # Leer el nuevo parámetro 'top_n' de la URL, con 10 como valor por defecto
        top_n = request.args.get('top_n', 10, type=int)

        if category and category.lower() == 'all':
            category = None

        data = dashboard_service.get_sales_analysis_data(category, top_n)

        if data:
            return jsonify(data)
        else:
            return jsonify({"error": "No se pudieron obtener los datos"}), 500

    except Exception as e:
        logger.error(f"Error en el endpoint /data/sales_analysis: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500


@api_bp.route('/categories')
def get_categories():
    try:
        categories = dashboard_service.get_all_categories()
        return jsonify(categories)
    except Exception as e:
        logger.error(f"Error en el endpoint /categories: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500
