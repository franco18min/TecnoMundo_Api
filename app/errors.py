from flask import jsonify


def register_error_handlers(app):
    @app.errorhandler(404)
    def not_found_error(error):
        return jsonify({"error": "Recurso no encontrado", "message": "La URL solicitada no existe."}), 404

    @app.errorhandler(500)
    def internal_error(error):
        app.logger.error(f"Error interno del servidor: {error}", exc_info=True)
        return jsonify({"error": "Error interno del servidor", "message": "Ocurrió un problema inesperado."}), 500

    @app.errorhandler(400)
    def bad_request_error(error):
        return jsonify({"error": "Solicitud incorrecta",
                        "message": str(error.description) or "La solicitud no pudo ser entendida."}), 400
