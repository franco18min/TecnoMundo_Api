from flask import Blueprint, jsonify, request, current_app
from .services import data_service

api_bp = Blueprint('api', __name__)


@api_bp.route('/status')
def status():
    return jsonify(data_service.get_status())


@api_bp.route('/reload', methods=['POST'])
def reload():
    folder = current_app.config['DATA_FOLDER']
    status = data_service.load_and_process_data(folder)
    if status['datos_cargados']:
        return jsonify({"status": "success", "message": "Datos recargados.", "info": status})
    else:
        return jsonify({"status": "error", "message": "Fallo al recargar.", "error": status['error_carga']}), 500


@api_bp.route('/categorias')
def categorias():
    categorias = data_service.get_categorias()
    if categorias is None:
        return jsonify({"error": "Datos no disponibles."}), 503
    return jsonify(categorias)


@api_bp.route('/top-productos')
def top_productos():
    categoria = request.args.get('categoria', type=str)
    limite = request.args.get('limite', 10, type=int)

    if not categoria:
        return jsonify({"error": "Parámetro 'categoria' es requerido."}), 400

    productos = data_service.get_top_productos(categoria, limite)
    if productos is None:
        return jsonify({"error": "Datos no disponibles."}), 503
    if not productos:
        return jsonify({"error": f"Categoría '{categoria}' no encontrada."}), 404

    return jsonify(productos)

