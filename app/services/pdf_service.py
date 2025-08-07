# app/services/pdf_service.py
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.units import inch
from io import BytesIO
from datetime import datetime
import locale
import logging

logger = logging.getLogger(__name__)

# Establecer el locale en español para el nombre del mes
try:
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, 'Spanish_Spain.1252')
    except locale.Error:
        logger.warning("No se pudo establecer el locale en español para las fechas del PDF.")


class PDFService:
    def create_inventory_list_pdf(self, data, title="Informe de Inventario"):
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=inch / 2, leftMargin=inch / 2, topMargin=inch / 2,
                                bottomMargin=inch / 2)

        styles = getSampleStyleSheet()
        story = []

        # Título y fecha
        main_title = Paragraph(title, styles['h1'])
        story.append(main_title)

        date_str = datetime.now().strftime("%d de %B de %Y")
        date_para = Paragraph(f"Generado el: {date_str.capitalize()}", styles['Normal'])
        story.append(date_para)
        story.append(Spacer(1, 0.25 * inch))

        # Preparar datos para la tabla
        table_data = [["Producto", "Stock Actual", "Ventas (30d)", "Estado"]]

        for item in data:
            # CORRECCIÓN: Convertir stock a entero para eliminar el ".0"
            stock_actual_int = int(item.get('stock_actual', 0))

            table_data.append([
                Paragraph(item.get('nombre_del_producto', ''), styles['BodyText']),
                stock_actual_int,
                item.get('unidades_vendidas_30d', 0),
                item.get('estado', 'N/A')
            ])

        # Crear y estilizar la tabla
        table = Table(table_data, colWidths=[3.5 * inch, 1.2 * inch, 1.2 * inch, 1.6 * inch])

        # Estilo base de la tabla
        style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#005f6b")),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor("#00b8d4")),
            ('LEFTPADDING', (0, 0), (0, -1), 10),
            ('RIGHTPADDING', (-1, 0), (-1, -1), 10),
            ('ALIGN', (0, 1), (0, -1), 'LEFT'),
        ])

        # Mapeo de colores de estado (coincidiendo con el dashboard)
        status_colors = {
            'Sin Stock': colors.HexColor("#FECACA"),  # bg-red-100
            'Riesgo de Quiebre': colors.HexColor("#FEE2E2"),
            'Inventario Estancado': colors.HexColor("#E5E7EB"),  # bg-gray-200
            'Lenta Rotación': colors.HexColor("#FDE68A"),  # bg-yellow-100
            'Alta Rotación': colors.HexColor("#BFDBFE"),  # bg-blue-100
            'Rotación Saludable': colors.HexColor("#A7F3D0")  # bg-green-100
        }

        # Colorear filas según el estado
        for i, row in enumerate(data):
            status = row['estado']
            color = status_colors.get(status)
            if color:
                style.add('BACKGROUND', (0, i + 1), (-1, i + 1), color)

        table.setStyle(style)

        story.append(table)
        doc.build(story)

        buffer.seek(0)
        return buffer
