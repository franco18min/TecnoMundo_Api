from flask import Blueprint, render_template, jsonify, request, abort, send_file
from app.services.dashboard_service import DashboardService
from app.services.pdf_service import PDFService
from .extensions import cache
import logging
from datetime import datetime

api_bp = Blueprint('api', __name__, template_folder='templates', static_folder='static')
actions_bp = Blueprint('actions', __name__)  # Nuevo Blueprint para acciones
dashboard_service = DashboardService()
pdf_service = PDFService()
logger = logging.getLogger(__name__)


@api_bp.route('/')
def index():
    return render_template('dashboard.html')


@api_bp.route('/data/sales_analysis')
@cache.cached(timeout=300, query_string=True)
def get_sales_analysis_data():
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
@cache.cached(timeout=3600)
def get_inventory_health_report():
    try:
        report_data = dashboard_service.get_inventory_health_report_data()
        if report_data:
            return jsonify(report_data)
        else:
            return jsonify({"error": "No se pudo generar el informe"}), 500
    except Exception as e:
        logger.error(f"Error en el endpoint /reports/inventory_health: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500


@api_bp.route('/reports/sales_date_range')
@cache.cached(timeout=3600)
def get_sales_date_range():
    try:
        date_range = dashboard_service.get_sales_date_range()
        if date_range:
            return jsonify(date_range)
        else:
            return jsonify({"error": "No se pudo obtener el rango de fechas"}), 404
    except Exception as e:
        logger.error(f"Error en /reports/sales_date_range: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500


@api_bp.route('/reports/sales_trend')
@cache.cached(timeout=300, query_string=True)
def get_sales_trend():
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')

    if not start_date_str or not end_date_str:
        abort(400, description="Los parámetros 'start_date' y 'end_date' son requeridos.")

    try:
        datetime.strptime(start_date_str, '%Y-%m-%d')
        datetime.strptime(end_date_str, '%Y-%m-%d')

        trend_data = dashboard_service.get_sales_trend(start_date_str, end_date_str)
        if trend_data is not None:
            return jsonify(trend_data)
        else:
            return jsonify({"error": "No se pudieron obtener los datos de tendencia"}), 500
    except ValueError:
        abort(400, description="Formato de fecha inválido. Use YYYY-MM-DD.")
    except Exception as e:
        logger.error(f"Error en /reports/sales_trend: {e}")
        return jsonify({"error": "Error interno del servidor"}), 500


# --- ENDPOINT DE EXPORTACIÓN MODIFICADO ---
@actions_bp.route('/export/inventory_pdf', methods=['POST'])
def export_inventory_pdf():
    try:
        filters = request.get_json()
        if not filters or not filters.get('statuses'):
            return jsonify({"error": "Debe seleccionar al menos un estado de inventario."}), 400

        data = dashboard_service.get_filtered_inventory_data(filters)
        if not data:
            return jsonify({"error": "No se encontraron datos con los filtros seleccionados."}), 404

        title = "Informe de Inventario Filtrado"  # Título genérico
        pdf_buffer = pdf_service.create_inventory_list_pdf(data, title)

        date_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"informe-inventario-{date_str}.pdf"

        return send_file(
            pdf_buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/pdf'
        )
    except Exception as e:
        logger.error(f"Error al exportar el PDF de inventario: {e}")
        return jsonify({"error": "Error interno del servidor al generar el PDF"}), 500
