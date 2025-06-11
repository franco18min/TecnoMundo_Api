import pandas as pd
import numpy as np
import os

# Carpeta de origen (donde está el archivo original)
carpeta_origen = "archive"

# Carpeta de destino para el archivo procesado
carpeta_destino = "archive_final"

# Verificar que la carpeta de origen exista
if not os.path.exists(carpeta_origen):
    raise FileNotFoundError(f"La carpeta '{carpeta_origen}' no existe.")

# Buscar archivos soportados
archivos_soportados = []
for archivo in os.listdir(carpeta_origen):
    if archivo.lower().endswith(('.xlsx', '.xls', '.csv', '.json')):
        archivos_soportados.append(archivo)

if not archivos_soportados:
    raise FileNotFoundError("No hay archivos soportados en la carpeta 'archive'.")

# Tomar el primer archivo encontrado
nombre_archivo = archivos_soportados[0]
ruta_completa = os.path.join(carpeta_origen, nombre_archivo)
nombre_base, extension = os.path.splitext(nombre_archivo)

print(f"Procesando archivo: {nombre_archivo}")

try:
    # Leer el archivo
    if extension.lower() in ('.xlsx', '.xls'):
        df = pd.read_excel(ruta_completa, engine='openpyxl')
    elif extension.lower() == '.csv':
        df = pd.read_csv(ruta_completa)
    elif extension.lower() == '.json':
        df = pd.read_json(ruta_completa)
    else:
        raise ValueError(f"Formato no soportado: {extension}")

    # agrega la columna 'Categoria' y que tenga los mismo valores de la columna 'Nombre del Producto'
    df['Categoria'] = df['Nombre del Producto']

    # str columna 'Categoria' y 'Nombre del Producto'
    df['Categoria'] = df['Categoria'].astype(str)
    df['Nombre del Producto'] = df['Nombre del Producto'].astype(str)

    # agregar la columna 'Categoria' y agregar la cadena 'Modulo' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'modulo' en su nombre y que no tengan la palabra 'servicio',si no cumple esta condicion se deja el valor original
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('modulo', case=False) & ~df['Nombre del Producto'].str.contains(
            'servicio', case=False), 'Modulo', df['Nombre del Producto'])

    # agregar en la columna 'Categoria'  la cadena 'Modulo Original' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'modulo' y la cadena 'original' en su nombre y que no tengan la palabra 'servicio',si no cumple esta condicion se deja el valor original
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('modulo', case=False) & df['Nombre del Producto'].str.contains(
            'original', case=False) & ~df['Nombre del Producto'].str.contains('servicio', case=False),
        'Modulo Original', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Vidrio Glass' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Vidrio Glass' en su nombre y que no tengan la palabra 'servicio',si no cumple esta condicion se deja el valor original
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('Vidrio Glass', case=False) & ~df['Nombre del Producto'].str.contains(
            'servicio', case=False), 'Vidrio Glass', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Pin Carga' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'pin carga' en su nombre y que no tengan la palabra 'servicio',si no cumple esta condicion se deja el valor original
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('pin carga', case=False) & ~df['Nombre del Producto'].str.contains(
            'servicio', case=False), 'Pin Carga', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Placa de Carga' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'placa de carga' en su nombre y que no tengan la palabra 'servicio',si no cumple esta condicion se deja el valor original
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('placa de carga', case=False) & ~df['Nombre del Producto'].str.contains(
            'servicio', case=False), 'Placa de Carga', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Tapa' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'tapa' en su nombre y que no tengan la palabra 'tapas',si no cumple esta condicion se deja el valor original
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('tapa', case=False) & ~df['Nombre del Producto'].str.contains('tapas',
                                                                                                             case=False),
        'Tapa', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Servicio' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'servicio' en su nombre,si no cumple esta condicion se deja el valor original
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('servicio', case=False), 'Servicio Tecnico',
                               df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Cable' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'cable' en su nombre y si no tienen las siguientes cadenas : Cargador, Conversor, JOYSTICK, AURICULAR, CARCASA, Pulsera, Adaptador, BASTON, TECLADO, Auriculares, Mouse, Pinza, Tester ,Zapatilla y Antena. si no cumple esta condicion se deja el valor original
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('cable', case=False) & ~df['Nombre del Producto'].str.contains(
            'Cargador|Conversor|JOYSTICK|AURICULAR|CARCASA|Pulsera|Adaptador|BASTON|TECLADO|Auriculares|Mouse|Pinza|Tester|Zapatilla|Antena',
            case=False), 'Cable', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Camara de Seguridad' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Camara de Seguridad' en su nombre y si no tienen la siguiente cadena 'fuente'
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('Camara de Seguridad', case=False) & ~df[
        'Nombre del Producto'].str.contains('fuente', case=False), 'Camara de Seguridad', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Bateria' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Bateria' en su nombre y si no tienen las siguientes cadenas: Activador, Espatula, CARGADOR
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('bateria', case=False) & ~df['Nombre del Producto'].str.contains(
            'Activador|Espatula|CARGADOR', case=False), 'Bateria', df['Categoria'])

    # ver que nombre de producto se vendio mas veces con el nombre de la columna
    df['Nombre del Producto'].value_counts()

    # agregar en la columna 'Categoria'  la cadena 'Parlante' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Parlante' en su nombre y si no tienen las siguientes cadenas: Auriculares, Fuente
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('parlante', case=False) & ~df['Nombre del Producto'].str.contains(
            'Auriculares|Fuente', case=False), 'Parlante', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Teclado' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Teclado' en su nombre y si no tienen las siguientes cadenas: Mouse, Kit, Conversor
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('teclado', case=False) & ~df['Nombre del Producto'].str.contains(
            'Mouse|Kit|Conversor', case=False), 'Teclado', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Cargador' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Cargador' en su nombre y si no tienen las siguientes cadenas: CABEZAL, ACTIVADOR, Cable, PARLANTE, Fuente y SOPORTE
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('cargador', case=False) & ~df['Nombre del Producto'].str.contains(
            'ACTIVADOR|PARLANTE|Fuente|SOPORTE', case=False), 'Cargador', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Pila' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Pila' en su nombre y si no tienen las siguientes cadenas: JOYSTICK, CARGADOR, ESPEJO y Teclado
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('pila', case=False) & ~df['Nombre del Producto'].str.contains(
            'JOYSTICK|CARGADOR|ESPEJO|Teclado', case=False), 'Pila', df['Categoria'])

    # cambiar el valor  en columna 'categoria' y 'nombre del producto' donde sea igual a  'Consola Alien 16bits HDMI. Con 2 joysticks inalambricos. Excelente calidad de imagen. Podras jugar a los juegos de 16 bits en el televisor LCD o LED . La consola tiene mas de 500 juegos clasidos' a 'Consola Alien 16bits HDMI + 2 joysticks inalambricos.
    df['Categoria'] = np.where(df[
                                   'Categoria'] == 'Consola Alien 16bits HDMI. Con 2 joysticks inalambricos. Excelente calidad de imagen. Podras jugar a los juegos de 16 bits en el televisor LCD o LED . La consola tiene mas de 500 juegos clasidos',
                               'Consola Alien 16bits HDMI + 2 joysticks inalambricos', df['Categoria'])
    df['Nombre del Producto'] = np.where(df[
                                             'Nombre del Producto'] == 'Consola Alien 16bits HDMI. Con 2 joysticks inalambricos. Excelente calidad de imagen. Podras jugar a los juegos de 16 bits en el televisor LCD o LED . La consola tiene mas de 500 juegos clasidos',
                                         'Consola Alien 16bits HDMI + 2 joysticks inalambricos',
                                         df['Nombre del Producto'])

    # agregar en la columna 'Categoria'  la cadena 'Consola' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Consola' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('consola', case=False), 'Consola',
                               df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Mouse' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Mouse' en su nombre y si no tienen las siguientes cadenas: KIT, Conversor, Teclado y TOSTADORA
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('mouse', case=False) & ~df['Nombre del Producto'].str.contains(
            'KIT|Conversor|Teclado|TOSTADORA', case=False), 'Mouse', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Auriculares' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Auriculares' en su nombre y si no tienen las siguientes cadenas: HOLDER Y SOPORTE
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('auriculares', case=False) & ~df['Nombre del Producto'].str.contains(
            'HOLDER|SOPORTE', case=False), 'Auriculares', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Adaptador' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Adaptador' en su nombre y si no tienen las siguientes cadenas: Antena , CABEZAL , MICROSCOPIO y Tarjeta
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('adaptador', case=False) & ~df['Nombre del Producto'].str.contains(
            'Antena|CABEZAL|MICROSCOPIO|Tarjeta', case=False), 'Adaptador', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Film Glass' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Film Glass' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('film glass', case=False), 'Film Glass',
                               df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Auricular' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Auricular' en su nombre y si no tienen las siguientes cadenas: SOPORTE y Touch
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('auricular', case=False) & ~df['Nombre del Producto'].str.contains(
            'SOPORTE|Touch', case=False), 'Auricular', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Joystick' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Joystick' en su nombre y si no tienen las siguientes cadenas: analogico, analogo, membrana, consola, gatillos y soporte
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('joystick', case=False) & ~df['Nombre del Producto'].str.contains(
            'analogico|analogo|membrana|consola|soporte', case=False), 'Joystick', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Film Glass' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Film Glass' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('film glass', case=False), 'Film Glass',
                               df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Smartwatch' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Smartwatch' o 'Smart watch' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('smartwatch|smart watch', case=False),
                               'Smartwatch', df['Categoria'])

    # en la fila donde nombre de producto sea igual a  'Ariculares BT Skullcandy Jib True 2 Con Microfono Gris S1JTW-P744' cambiar el valor de la columna categoria a 'Auriculares'
    df['Categoria'] = np.where(
        df['Nombre del Producto'] == 'Auriculares BT Skullcandy Jib True 2 Con Microfono Gris S1JTW-P744',
        'Auriculares', df['Categoria'])

    # en la columna nombre de producto donde sea igual a 'Ariculares BT Skullcandy Jib True 2 Con Microfono Gris S1JTW-P744' cambiar el valor de la columna nombre de producto a 'Auriculares BT Skullcandy Jib True 2 Con Microfono Gris S1JTW-P744' y mostrar fila
    df['Nombre del Producto'] = np.where(
        df['Nombre del Producto'] == 'Ariculares BT Skullcandy Jib True 2 Con Microfono Gris S1JTW-P744',
        'Auriculares BT Skullcandy Jib True 2 Con Microfono Gris S1JTW-P744', df['Nombre del Producto'])

    # agregar en la columna 'Categoria'  la cadena 'Adaptador' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Adaptador' en su nombre y si no tienen las siguientes cadenas: Cargador, Cable, Tarjeta, Adaptador Headphone Jack iPhone 7 Lightning A 3.5mm ADAN005,Mini Adaptador USB Wifi tp-link TL-WN823N 300Mbps 39mm, Adaptador Nano USB a Wireless 150 Mbps Mercusys by Tp-Link MW150US N150 NANO, Adaptador Inalambrico Wifi Usb Red Noga NG-Uw06 Antena 150mbps 2,4GHZ, CARGADOR CABEZAL ADAPTADOR PUERTO USB + TYPE C 6.1A 5.8A IBEK IB-611PD20W QC 3.0 A+C,MICROSCOPIO y Antena
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('adaptador', case=False) & ~df['Nombre del Producto'].str.contains(
            'Cargador|Cable|Tarjeta|Adaptador Headphone Jack iPhone 7 Lightning A 3.5mm ADAN005|Mini Adaptador USB Wifi tp-link TL-WN823N 300Mbps 39mm|Adaptador Nano USB a Wireless 150 Mbps Mercusys by Tp-Link MW150US N150 NANO|Adaptador Inalambrico Wifi Usb Red Noga NG-Uw06 Antena 150mbps 2,4GHZ|CARGADOR CABEZAL ADAPTADOR PUERTO USB + TYPE C 6.1A 5.8A IBEK IB-611PD20W QC 3.0 A+C|MICROSCOPIO|Antena',
            case=False), 'Adaptador', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Soporte' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Soporte' o 'Holder' en su nombre y si no tienen las siguientes cadenas: Lector de Codigo de barras Laser USB 1D CON SOPORTE PIE Netmak NM-LC401-S y JOYSTICK
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('soporte|holder', case=False) & ~df['Nombre del Producto'].str.contains(
            'Lector de Codigo de barras Laser USB 1D CON SOPORTE PIE Netmak NM-LC401-S|JOYSTICK', case=False),
        'Soporte', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Disco' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Disco' en su nombre y si no tienen las siguientes cadenas: CARCASA, CASE Y Notebook
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('disco', case=False) & ~df['Nombre del Producto'].str.contains(
            'CARCASA|CASE|Notebook', case=False), 'Disco', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Accesorio noebook' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Funda' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('funda', case=False), 'Accesorio notebook',
                               df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Impresora' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Impresora' en su nombre y no tiene las siguientes cadenas: cable
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('impresora', case=False) & ~df['Nombre del Producto'].str.contains(
            'cable', case=False), 'Impresora', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Instrumento Musical' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Armonica' o 'Organo' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('armonica|organo', case=False),
                               'Instrumento Musical', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Juguete' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Auto Control Remoto' o 'Piano' o 'Children' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('auto control remoto|piano|children', case=False),
                               'Juguete', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Libreria' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Contadora' o 'Cinta de Embalaje' o 'Calculadora Cientifica' en su nombre
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('contadora|cinta de embalaje|calculadora cientifica', case=False),
        'Libreria', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Luces' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Manguera de Led' o 'Lampara' o 'Aro Luz' o 'Espejo' o 'Uñas' o 'Light' o 'Led RGB' o 'Tira Led' o 'Puntero Laser' o 'Lampara de Pared' o 'Parlante Luz Bola' o 'Reflector' o 'Luz Led' o 'PROYECTOR' o 'Reflector' o 'Lamparita' o 'Proyector Nebulosa' o 'Kit de Iluminacion' o 'Foco' o 'Difusor Fotografico' o 'Luz' o 'Linterna' o 'Luces' o 'Lamparas' o 'Flash de Led' en su nombre y si no tienen las siguientes cadenas: cable, Tripode Metalico 0.55m PARA ARO LUZ led HX-055M, Radio, Balanza, LAMPARA CURADO UV Con Extractor Mc18 MECHANIC, Mouse, Parlante, Mousepad, Tester, adaptador, arcade, cooler, base, consola, kit, microfono y teclado
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains(
        'manguera de led|lampara|aro luz|espejo|uñas|light|led rgb|tira led|puntero laser|lampara de pared|parlante luz bola|reflector|luz led|PROYECTOR|reflector|lamparita|proyector nebulosa|kit de iluminacion|foco|difusor fotografico|luz|linterna|luces|lamparas|flash de led',
        case=False) & ~df['Nombre del Producto'].str.contains(
        'cable|Tripode Metalico 0.55m PARA ARO LUZ led HX-055M|Radio|Balanza|LAMPARA CURADO UV Con Extractor Mc18 MECHANIC|Mouse|Parlante|Mousepad|Tester|adaptador|arcade|cooler|base|consola|kit|microfono|teclado',
        case=False), 'Luces', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Memoria Ram' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'DDR3' o 'DDR4' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('ddr3|ddr4', case=False), 'Memoria Ram',
                               df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Tarjeta de Memoria' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Tarjeta de Memoria' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('tarjeta de memoria', case=False),
                               'Tarjeta de Memoria', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Microfono' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Microfono' en su nombre y no tiene las siguientes cadenas: camara, estabilizador, auricular, auriculares, parlante
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('microfono', case=False) & ~df['Nombre del Producto'].str.contains(
            'camara|estabilizador|auricular|auriculares|parlante', case=False), 'Microfono', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Mochila' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Mochila' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('mochila', case=False), 'Mochila',
                               df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Monitor' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Monitor' en su nombre y no tienen las siguientes cadenas: cable, base, espuma
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('monitor', case=False) & ~df['Nombre del Producto'].str.contains(
            'cable|base|espuma', case=False), 'Monitor', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Movilidad' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Monopatin' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('monopatin', case=False), 'Movilidad',
                               df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Accesorio notebook' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Base Cooler' o 'Soporte de Mesa', 'maletin', 'disco rigido' y 'Notebook' en su nombre y  al tener la palabra notebook es necesario comprobar que la primera cadena del valor no sea la cadena notebook
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('Base Cooler|soporte de mesa|notebook|maletin|Disco Rigido',
                                               case=False) & ~df['Nombre del Producto'].str.contains('notebook',
                                                                                                     case=False),
        'Accesorio notebook', df['Categoria'])

    # en las filas donde nombre de producto tenga si o si la cadena base y notebook estrictamente, reemplazar por 'Accesorio notebook' en la columna categoria
    df['Categoria'] = np.where((df['Nombre del Producto'].str.contains('base', case=False)) & (
        df['Nombre del Producto'].str.contains('notebook', case=False)), 'Accesorio notebook', df['Categoria'])

    # en las filas donde nombre de producto tenga si o si la cadena maletin y notebook estrictamente, reemplazar por 'Accesorio notebook' en la columna categoria
    df['Categoria'] = np.where((df['Nombre del Producto'].str.contains('maletin', case=False)) & (
        df['Nombre del Producto'].str.contains('notebook', case=False)), 'Accesorio notebook', df['Categoria'])

    # en las filas donde nombre de producto tenga si o si la cadena disco rigido y notebook estrictamente, reemplazar por 'Accesorio notebook' en la columna categoria
    df['Categoria'] = np.where((df['Nombre del Producto'].str.contains('disco rigido', case=False)) & (
        df['Nombre del Producto'].str.contains('notebook', case=False)), 'Accesorio notebook', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Monitor' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Monitor' en su nombre y no tienen las siguientes cadenas: cable, base, espuma
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('monitor', case=False) & ~df['Nombre del Producto'].str.contains(
            'cable|base|espuma', case=False), 'Monitor', df['Categoria'])

    # en las filas de nombre de producto donde su primera cadena sea la cadena 'notebook' reemplazar en su columna categoria por 'Notebook'
    df['Categoria'] = np.where(df['Nombre del Producto'].str.lower().str.startswith('notebook', na=False), 'Notebook',
                               df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Malla' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Malla' en su nombre y no tiene la cadena cable
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('malla', case=False) & ~df['Nombre del Producto'].str.contains('cable',
                                                                                                              case=False),
        'Malla', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Mousepad' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Mouse Pad' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('mouse pad', case=False), 'Mousepad',
                               df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Fuente' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Fuente' en su nombre y no tiene las siguientes cadenas: cable, cooler, adaptador
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('fuente', case=False) & ~df['Nombre del Producto'].str.contains(
            'cable|cooler|adaptador', case=False), 'Fuente', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Gabinete' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Gabinete' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('gabinete', case=False), 'Gabinete',
                               df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Gabinete' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Cooler' en su nombre y no tiene las siguientes cadenas: base, joystick
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('cooler', case=False) & ~df['Nombre del Producto'].str.contains(
            'base|joystick', case=False), 'Cooler', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Fuente' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Fuente' en su nombre y no tiene las siguientes cadenas: cable, chromecast, transformador, universal, turbina, adaptador, camara, sunshine
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('fuente', case=False) & ~df['Nombre del Producto'].str.contains(
            'cable|chromecast|transformador|universal|turbina|adaptador|camara|sunshine', case=False), 'Fuente',
        df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Herramienta' en cada fila si los valores  de la columna 'Nombre de Producto' tienen estrictamente las siguientes dos cadenas: fuente y sunshine
    df['Categoria'] = np.where((df['Nombre del Producto'].str.contains('fuente', case=False)) & (
        df['Nombre del Producto'].str.contains('sunshine', case=False)), 'Herramienta', df['Categoria'])

    # añadir en la columna categoria la cadena 'Herramienta' en cada fila si los valores de la columna 'Nombre del Producto' tienen las siguientes cadenas: pegamento, desoldante, pasta, flux, alcohol, resina, caja, pinza, palito, cuchilla, mango, paño, cinta, punta, organizador, holder basico, caja, localizador, desoldante, alicate, destornillador, tester, estaño, sopapa, cepillo, hilo, removedor, pua, flux, plancha, lampara uv, limpia contactos, activador, espatula, grasa, cocodrilo, pulpo, manta, termico, termica, protector, cortadora, spudger, espuma, goma, jeringa, pistola, stencil, gasa, precalentadora, gel, multimetro, microscopio, lubrimatic, alicate, drill, test, fuente, sunshine, cable digital, glue remove, soporte relife, korad, face id, wuzip, gavetero, sulfato, barlow, antiestatica, power z, compresor, estacion, holder universal, reparador de bateria, autoclave, isocket, curado, precalentadora, quitar, ultrasonido, soldado, cpu holder, lupa. Y que no tengan las siguientes cadenas : consola, cargador motorola, auriculares, malla cuero, mouse pad
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains(
        'pegamento|desoldante|pasta|flux|alcohol|resina|caja|pinza|palito|cuchilla|mango|paño|cinta|punta|organizador|holder basico|caja|localizador|desoldante|alicate|destornillador|tester|estaño|sopapa|cepillo|hilo|removedor|pua|flux|plancha|lampara uv|limpia contactos|activador|espatula|grasa|cocodrilo|pulpo|manta|termico|termica|protector|cortadora|spudger|espuma|goma|jeringa|pistola|stencil|gasa|precalentadora|gel|multimetro|microscopio|lubrimatic|alicate|drill|test|fuente|sunshine|cable digital|glue remove|soporte relife|korad|face id|wuzip|gavetero|sulfato|barlow|antiestatica|power z|compresor|estacion|holder universal|reparador de bateria|autoclave|isocket|curado|precalentadora|quitar|ultrasonido|soldado|cpu holder|lupa|lamina',
        case=False) & ~df['Nombre del Producto'].str.contains(
        'consola|cargador motorola|auriculares|malla cuero|mouse pad|botella plastica', case=False), 'Herramienta',
                               df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Impresora' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Impresora' en su nombre y no tiene las siguientes cadenas: cable
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('impresora', case=False) & ~df['Nombre del Producto'].str.contains(
            'cable', case=False), 'Impresora', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Instrumento Musical' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Armonica' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('armonica', case=False), 'Instrumento Musical',
                               df['Categoria'])

    # agregar en la columna categoria la cadena 'Joystick' en cada fila si los valores de la columna Código con valor C6239, CO11271
    df['Categoria'] = np.where(df['Código'].str.contains('C6239|C6224', case=False), 'Joystick', df['Categoria'])

    # Agregar en la columna 'Categoria'  la cadena 'Juguete' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Juego de Memoria' o 'Teclado Piano Organo' o 'Auto Control Remoto' en su nombre
    df['Categoria'] = np.where(
        df['Nombre del Producto'].str.contains('juego de memoria|teclado piano organo|auto control remoto', case=False),
        'Juguete', df['Categoria'])

    # agregar en la columna 'Categoria'  la cadena 'Chip' en cada fila si los valores  de la columna 'Nombre de Producto' tienen la cadena 'Chip' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.contains('chip', case=False), 'Chip', df['Categoria'])

    # agregar la cadena 'Flex' en la columna categoria en cada fila que tenga estrictamente la cadena 'Flex' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.lower().str.startswith('flex', na=False), 'Flex',
                               df['Categoria'])

    # agregar la cadena 'Flex' en la columna categoria en cada fila que tenga estrictamente la cadena 'FPC' en su nombre
    df['Categoria'] = np.where(df['Nombre del Producto'].str.lower().str.startswith('fpc', na=False), 'Flex',
                               df['Categoria'])

    # Agregar la cadena en la columna categoria la cadena "Badeja SIM" si la columna "Nombre del Producto" contiene la cadena "bandeja sim"
    df.loc[df['Nombre del Producto'].str.contains('bandeja sim', case=False), 'Categoria'] = 'Badeja SIM'

    # Agregar la cadena en la columna categoria la cadena "HUB" si la columna "Nombre del Producto" contiene la cadena "hub" y que no tenga las cadenas : adaptador
    df.loc[
        df['Nombre del Producto'].str.contains('hub', case=False) & ~df['Nombre del Producto'].str.contains('adaptador',
                                                                                                            case=False), 'Categoria'] = 'HUB'

    # Agregar la cadena en la columna categoria la cadena "Camara de Seguridad" si la columna "Nombre del Producto" tiene estrictamente la cadena "camara smart"
    df.loc[df['Nombre del Producto'].str.contains('camara smart', case=False), 'Categoria'] = 'Camara de Seguridad'

    # Agregar la cadena en la columna categoria la cadena "Camara de Seguridad" si la columna "Nombre del Producto" tiene estrictamente las cadenas camara y interior o exterior
    df.loc[df['Nombre del Producto'].str.contains('camara', case=False) & df['Nombre del Producto'].str.contains(
        'interior|exterior', case=False), 'Categoria'] = 'Camara de Seguridad'

    # Agregar la cadena en la columna categoria la cadena "Camara de Seguridad" si la columna "Nombre del Producto" tiene estrictamente las cadenas "camara" y "espia"
    df.loc[
        df['Nombre del Producto'].str.contains('camara', case=False) & df['Nombre del Producto'].str.contains('espia',
                                                                                                              case=False), 'Categoria'] = 'Camara de Seguridad'

    # añadir la cadena "Camara de Seguridad" en la columna categoria si la columna "Nombre del Producto" tiene estrictamente las cadenas camara y interior o exterior
    df.loc[df['Nombre del Producto'].str.contains('camara', case=False) & df['Nombre del Producto'].str.contains(
        'interior|exterior', case=False), 'Categoria'] = 'Camara de Seguridad'

    # Agregar la cadena en la columna categoria la cadena "Camara Web" si la columna "Nombre del Producto" tiene estrictamente la cadena "camara web"
    df.loc[df['Nombre del Producto'].str.contains('camara web', case=False), 'Categoria'] = 'Camara Web'

    # Agregar la cadena en la columna categoria la cadena "Pantalla Tactil" si enla columna "Nombre del Producto" tiene estrictamente la cadena "tactil"
    df.loc[df['Nombre del Producto'].str.contains('^tactil', case=False), 'Categoria'] = 'Tactil'

    # Agregar la cadena en la columna categoria la cadena "Kit gaming" si la columna "Nombre del Producto" contiene estrictamente la cadena "gaming" y "combo"
    df.loc[
        df['Nombre del Producto'].str.contains('gaming', case=False) & df['Nombre del Producto'].str.contains('combo',
                                                                                                              case=False), 'Categoria'] = 'Kit Gaming'

    # agregar la cadena "Luces" en la columna categoria si la columna "Nombre del Producto" contiene la cadena "KIT COMBO TIRA DE LED RGB LED-Y3528RD"
    df.loc[df['Nombre del Producto'].str.contains('KIT COMBO TIRA DE LED RGB LED-Y3528RD',
                                                  case=False), 'Categoria'] = 'Luces'

    # Agregar la cadena en la columna categoria la cadena "Flex" si la columna "Nombre del Producto" contiene estrictamente la cadena "boton home" o "boton encendido"
    df.loc[df['Nombre del Producto'].str.contains('boton home|boton encendido', case=False), 'Categoria'] = 'Flex'

    # Agregar la cadena en la columna categoria la cadena "Tactil" si la columna "Nombre del Producto" contiene la cadena "touch" y no tiene ninguna de las siguientes cadenas: lampara, espejo
    df.loc[df['Nombre del Producto'].str.contains('touch', case=False) & ~df['Nombre del Producto'].str.contains(
        'lampara|espejo', case=False), 'Categoria'] = 'Tactil'

    # Agregar la cadena en la columna categoria la cadena "Router" si la columna "Nombre del Producto" tiene la cadena router y no tiene las cadenas: fuente
    df.loc[
        df['Nombre del Producto'].str.contains('router', case=False) & ~df['Nombre del Producto'].str.contains('fuente',
                                                                                                               case=False), 'Categoria'] = 'Router'

    # Agregar la cadena en la columna categoria la cadena "Smartwatch" si la columna "Nombre del Producto" contiene estrictamente la cadena smartwatch, smart watch, reloj y sin las cadenas: malla y cargador
    df.loc[df['Nombre del Producto'].str.contains('smartwatch|smart watch|reloj', case=False) & ~df[
        'Nombre del Producto'].str.contains('malla|cargador', case=False), 'Categoria'] = 'Smartwatch'

    # Agregar la cadena en la columna categoria la cadena "Herramienta" si la columna "Nombre del Producto" contiene estrictamente la cadena: base separadora, hoja de puntos, botella plastica
    df.loc[df['Nombre del Producto'].str.contains('base separadora|hoja de puntos|botella plastica',
                                                  case=False), 'Categoria'] = 'Herramienta'

    # cambiar valor a Auricular
    df.loc[df['Categoria'].str.contains('Ariculares BT Skullcandy Jib True 2 Con Microfono Gris S1JTW-P744',
                                        case=False), 'Categoria'] = 'Auricular'

    # Agregar la cadena en la columna categoria la cadena "Pendrive" si la columna "Nombre del Producto" contiene estrictamente la cadena pendrive, pendrive
    df.loc[df['Nombre del Producto'].str.contains('pendrive|pen drive', case=False), 'Categoria'] = 'Pendrive'

    # añadir la cadena "Servicio Tecnico" en la columna categoria si la columna "Nombre del Producto" tiene estrictamente la cadena serv tec o servicio tecnico
    df.loc[df['Nombre del Producto'].str.contains('serv tec|servicio tecnico',
                                                  case=False), 'Categoria'] = 'Servicio Tecnico'

    # Agregar la cadena en la columna categoria la cadena "Conversor" si la columna "Nombre del Producto" contiene estrictamente la cadena conversor y no tiene la cadena nisuta
    df.loc[df['Nombre del Producto'].str.contains('conversor', case=False) & ~df['Nombre del Producto'].str.contains(
        'nisuta', case=False), 'Categoria'] = 'Conversor'

    # agregar la cadena mouse pad en la columna categoria, si la columna "Nombre del Producto" tiene estrictamente la cadena pad y si en su columna de caegoria no tiene la cadena Herramienta
    df.loc[df['Nombre del Producto'].str.contains('^pad', case=False) & ~df['Categoria'].str.contains('Herramienta',
                                                                                                      case=False), 'Categoria'] = 'Mouse Pad'

    # agregar la cadena "Lapiz" en la columna categoria si la columna "Nombre del Producto" contiene estrictamente la cadena lapiz y si en su columna categoria no tiene la cadena Herramienta
    df.loc[df['Nombre del Producto'].str.contains('lapiz', case=False) & ~df['Categoria'].str.contains('Herramienta',
                                                                                                       case=False), 'Categoria'] = 'Lapiz'

    # para el producto con el codigo AC169 agregar la cadena "Lapiz" en la columna Lapiz
    df.loc[df['Código'] == 'AC169', 'Categoria'] = 'Lapiz'

    # agregar la cadena Smartwatch en la columna cateogir al producto con el codigo RE9494
    df.loc[df['Código'] == 'RE9494', 'Categoria'] = 'Smartwatch'

    # agregar la cadena "Radio" en la columna categoria si la columna "Nombre del Producto" tiene estrictamente la cadena radio como primera cadena
    df.loc[df['Nombre del Producto'].str.contains('^radio', case=False), 'Categoria'] = 'Radio'

    # añadir la cadena "Convertidor Smart" en la columna categoria si la columna "Nombre del Producto" contiene la cadena convertidor o tv stick o tv box
    df.loc[df['Nombre del Producto'].str.contains('convertidor|tv stick|tv box',
                                                  case=False), 'Categoria'] = 'Convertidor Smart'

    # Agregar la cadena en la columna categoria la cadena "Badeja SIM" si la columna "Nombre del Producto" contiene la cadena "bandeja sim"
    df.loc[df['Nombre del Producto'].str.contains('bandeja sim', case=False), 'Categoria'] = 'Badeja SIM'

    # Agregar la cadena en la columna categoria la cadena "HUB" si la columna "Nombre del Producto" contiene la cadena "hub" y que no tenga las cadenas : adaptador
    df.loc[
        df['Nombre del Producto'].str.contains('hub', case=False) & ~df['Nombre del Producto'].str.contains('adaptador',
                                                                                                            case=False), 'Categoria'] = 'HUB'
    # Agregar la cadena en la columna categoria la cadena "Lente Camara" si la columna "Nombre del Producto" contiene la cadena "lente camara"
    df.loc[df['Nombre del Producto'].str.contains('lente camara', case=False), 'Categoria'] = 'Lente Camara'

    # Agregar la cadena en la columna categoria la cadena "Celular" en las filas que tengan estrictamente como primera cadena, la cadena "celular" en la columan nombre del producto
    df.loc[df['Nombre del Producto'].str.contains('^celular', case=False), 'Categoria'] = 'Celular'

    # añadir la cadena "Pendrive" en la columna categoria si la columna "Nombre del Producto" contiene la cadena pendrive o pre drive o pen drive
    df.loc[df['Nombre del Producto'].str.contains('pendrive|pre drive|pen drive', case=False), 'Categoria'] = 'Pendrive'

    # agrergar la cadena "Herramienta" en la columna categoria si la columna "Nombre del Producto" contiene las siguientes cadenas: hoja de puntos, base separadora, botella plastica
    df.loc[df['Nombre del Producto'].str.contains('hoja de puntos|base separadora|botella plastica',
                                                  case=False), 'Categoria'] = 'Herramienta'

    # Agregar la cadena en la columna categoria la cadena "Camara Web" si la columna "Nombre del Producto" tiene estrictamente la cadena camara web
    df.loc[df['Nombre del Producto'].str.contains('camara web', case=False), 'Categoria'] = 'Camara Web'

    # Agregar la cadena en la columna categoria la cadena "Tactil" si en la columna "Nombre del Producto" tiene estrictamente la cadena "tactil" como primera cadena de su valor
    df.loc[df['Nombre del Producto'].str.contains('^tactil', case=False), 'Categoria'] = 'Tactil'

    # Agregar la cadena en la columna categoria la cadena "Tablet" si en la columna "Nombre del Producto" tiene estrictamente la cadena tablet como primera cadena
    df.loc[df['Nombre del Producto'].str.contains('^tablet', case=False), 'Categoria'] = 'Tablet'

    # agregar la cadena "Lapiz optico" en la columna categoria si la columna "Nombre del Producto" contiene estrictamente la cadena lapiz como primera cadena de su valor
    df.loc[df['Nombre del Producto'].str.contains('^lapiz', case=False), 'Categoria'] = 'Lapiz Optico'

    # Agregar la cadena en la columna categoria la cadena "Conversor" si la columna "Nombre del Producto" contiene la cadena conversor
    df.loc[df['Nombre del Producto'].str.contains('conversor', case=False), 'Categoria'] = 'Conversor'

    # añadir la cadena "Boton" en la columna categoria si la columna "Nombre del Producto" contiene estrictamente como primera cadena, la cadena "boton"
    df.loc[df['Nombre del Producto'].str.contains('^boton', case=False), 'Categoria'] = 'Boton'

    # reemplazar en la columna nombre del producto ,si una cadena es igual a smartwach , reemplazar esa cadena por smarwatch
    df['Nombre del Producto'] = df['Nombre del Producto'].str.replace('smartwach', 'Smartwatch', case=False)

    # añadir la cadena "Smartwatch" en la columna categoria si la columna "Nombre del Producto" contiene estrictamente alguna de las siguientes cadenas: reloj inteligente, reloj smartwatch
    df.loc[df['Nombre del Producto'].str.contains('reloj inteligente|reloj smartwatch',
                                                  case=False), 'Categoria'] = 'Smartwatch'

    # Cambiar a este producto en su columna categoria por la cadena Auricular
    df.loc[df['Código'] == 'AU3292', 'Categoria'] = 'Auricular'

    # añadir la cadena "Radio" en la columna categoria si la columna "Nombre del Producto" contiene estrictamente la cadena radio como primera cadena
    df.loc[df['Nombre del Producto'].str.contains('^radio', case=False), 'Categoria'] = 'Radio'

    # cambiar la categoria de ese producto por la cadena parlante
    df.loc[df['Nombre del Producto'].str.contains('barra sonido', case=False), 'Categoria'] = 'Parlante'

    # agregar la cadena "Estabilizadores" en la columna categoria si la columna "Nombre del Producto" contiene la cadena estabilizador
    df.loc[df['Nombre del Producto'].str.contains('estabilizador', case=False), 'Categoria'] = 'Estabilizador'

    # agregar la cadena "Extensor Wifi" en la columna categoria si la columna "Nombre del Producto" contiene la cadena repetidor o extensor y no contiene las siguientes cadenas: cable, hdmi
    df.loc[df['Nombre del Producto'].str.contains('repetidor|extensor', case=False) & ~df[
        'Nombre del Producto'].str.contains('cable|hdmi', case=False), 'Categoria'] = 'Extensor Wifi'

    # agregar la cadena Adaptador en la columna categoria si la columna "Nombre del Producto" contiene la cadena extensor de hdmi
    df.loc[df['Nombre del Producto'].str.contains('extensor de hdmi', case=False), 'Categoria'] = 'Adaptador'

    # agregar la cadena 'Receptor' en la columna categoria si la columna 'Nombre del Producto' contiene la cadena receptor
    df.loc[df['Nombre del Producto'].str.contains('receptor ', case=False), 'Categoria'] = 'Receptor'

    # agregar la cadena "Reloj Despertador" en la columna categoria si la columna "Nombre del Producto" contiene la cadena reloj despertador
    df.loc[df['Nombre del Producto'].str.contains('reloj despertador', case=False), 'Categoria'] = 'Reloj Despertador'

    # agregar la cadena "Camara de Seguridad" en la columna categoria si la columna "Nombre del Producto" contiene las cadenas camara y exterior
    df.loc[df['Nombre del Producto'].str.contains('camara', case=False) & df['Nombre del Producto'].str.contains(
        'exterior', case=False), 'Categoria'] = 'Camara de Seguridad'

    # a esos valores cambiar su categoria a Camara de Seguridad
    df.loc[df['Nombre del Producto'].str.contains('camara', case=False) & df['Nombre del Producto'].str.contains('wifi',
                                                                                                                 case=False) & ~
           df['Categoria'].str.contains('Camara de Seguridad', case=False), 'Categoria'] = 'Camara de Seguridad'
    df[df['Nombre del Producto'].str.contains('camara', case=False) & df['Nombre del Producto'].str.contains('wifi',
                                                                                                             case=False) & ~
       df['Categoria'].str.contains('Camara de Seguridad', case=False)]

    # a ese producto cambiarle la categoria a Camara de Seguridad
    df.loc[df['Nombre del Producto'].str.contains('camara smart', case=False), 'Categoria'] = 'Camara de Seguridad'

    # agregar la cadena "Router" en la columna categoria si la columna "Nombre del Producto" contiene la cadena router y no tiene la cadena fuente
    df.loc[
        df['Nombre del Producto'].str.contains('router', case=False) & ~df['Nombre del Producto'].str.contains('fuente',
                                                                                                               case=False), 'Categoria'] = 'Router'

    # agregar la cadena "Vidrio templado" en la columna categoria si la columna "Nombre del Producto" contiene la cadena vidrio templado o film glass
    df.loc[df['Nombre del Producto'].str.contains('vidrio templado|film glass',
                                                  case=False), 'Categoria'] = 'Vidrio Templado'

    # agregar la cadena "Smartwatch" en la columna categoria si la columna "Nombre del Producto" contiene la cadena smart wach o smartwatch
    df.loc[df['Nombre del Producto'].str.contains('smart wach|smartwatch', case=False), 'Categoria'] = 'Smartwatch'

    # agregar la cadena 'Tactil' en la columna categoria si el valor tiene estrictamente la cadena touch como primera cadena de su valor
    df.loc[df['Nombre del Producto'].str.contains('^touch', case=False), 'Categoria'] = 'Tactil'

    # agregar la cadena "Tactil" en la columna categoria si la columna "Nombre del Producto" contiene la cadena touch philco
    df.loc[df['Nombre del Producto'].str.contains('touch philco', case=False), 'Categoria'] = 'Tactil'

    # agregar la cadena "Badeja SIM" en la columna categoria si la columna "Nombre del Producto" contiene la cadena lector sim
    df.loc[df['Nombre del Producto'].str.contains('sim', case=False), 'Categoria'] = 'Badeja SIM'

    # agregar la cadena "Lector" en la columna categoria si la columna "Nombre del Producto" contiene la cadena lector y no tiene las cadenas parlante y sim
    df.loc[df['Nombre del Producto'].str.contains('lector', case=False) & ~df['Nombre del Producto'].str.contains(
        'parlante|sim', case=False), 'Categoria'] = 'Lector'

    # agregar la cadena "Servicio Tecnico" en la columna categoria si la columna "Nombre del Producto" contiene la cadena serv tec
    df.loc[df['Nombre del Producto'].str.contains('serv tec', case=False), 'Categoria'] = 'Servicio Tecnico'

    # agregar la cadena "Adaptador" en la columna categoria si la columna "Nombre del Producto" contiene la cadena hdmi y su categoria es distinta de cable, adaptador, conversor, consola, luces
    df.loc[df['Nombre del Producto'].str.contains('hdmi', case=False) & ~df['Categoria'].str.contains(
        'cable|adaptador|conversor|consola|luces', case=False), 'Categoria'] = 'Adaptador'

    # agregar la cadena "Conversor" en la columna categoria si la columna "Nombre del Producto" contiene la cadena splitter
    df.loc[df['Nombre del Producto'].str.contains('splitter', case=False), 'Categoria'] = 'Conversor'

    # agregar la cadena "Conversor" en la columna categoria si la columna "Nombre del Producto" contiene la cadena capturadora hdmi
    df.loc[df['Nombre del Producto'].str.contains('capturadora hdmi', case=False), 'Categoria'] = 'Conversor'

    # agregar la cadena "Luces" en la columna categoria si la columna "Nombre del Producto" contiene la cadena neon
    df.loc[df['Nombre del Producto'].str.contains('neon', case=False), 'Categoria'] = 'Luces'

    # agregar la cadena "Luces" en la columna categoria si la columna "Nombre del Producto" contiene la cadena tira de led y no tiene la cadena transformador
    df.loc[df['Nombre del Producto'].str.contains('tira de led', case=False) & ~df['Nombre del Producto'].str.contains(
        'transformador', case=False), 'Categoria'] = 'Luces'

    # agregar la cadena "Luces" en la columna categoria si la columna "Nombre del Producto" contiene la cadena mini led
    df.loc[df['Nombre del Producto'].str.contains('mini led', case=False), 'Categoria'] = 'Luces'

    # agregar la cadena "Kit Gaming" en la columna categoria si la columna "Nombre del Producto" contiene la cadena gaming COMBO
    df.loc[df['Nombre del Producto'].str.contains('gaming COMBO', case=False), 'Categoria'] = 'Kit Gaming'

    # agregar la cadena "Tripode" en la columna categoria si la columna "Nombre del Producto" contiene la cadena tripode y si columna catgoria es distinta de Luces,Microfono
    df.loc[
        df['Nombre del Producto'].str.contains('tripode', case=False) & ~df['Categoria'].str.contains('Luces|Microfono',
                                                                                                      case=False), 'Categoria'] = 'Tripode'

    # agregar en la columna categoria la cadena 'Accesorio Disco' si la columna nombre del producto contiene la cadena 'case disco'
    df.loc[df['Nombre del Producto'].str.contains('case disco', case=False), 'Categoria'] = 'Accesorio Disco'

    # agregar en la columna categoria la cadena 'Repuesto Joystick' si la columna nombre del producto contiene la cadena 'analogico'
    df.loc[df['Nombre del Producto'].str.contains('analogico', case=False), 'Categoria'] = 'Repuesto Joystick'

    # Cambiar la categoria de esas filas encontradas a Mousepad
    df.loc[(df['Nombre del Producto'].str.contains('pad', case=False)) & (
        df['Categoria'].str.contains(r'\b\w+\b \b\w+\b \b\w+\b', regex=True)), 'Categoria'] = 'Mousepad'

    # cambiar la categoria de las filas que contengan la cadena 'analogo' a 'Repuesto Joystick'
    df.loc[df['Nombre del Producto'].str.contains('analogo', case=False), 'Categoria'] = 'Repuesto Joystick'

    # en esos valores cambiar su categoria a 'Repuesto Joystick' solo si tienen la cadena membrana y soporte
    df.loc[(df['Nombre del Producto'].str.contains('joystick', case=False)) & (~df['Categoria'].str.contains('Repuesto Joystick|Joystick|Consola')) & (df['Nombre del Producto'].str.contains('membrana|soporte',case=False)), 'Categoria'] = 'Repuesto Joystick'

    # Cambiar la categoria de los productos a 'Asistente Virtual' para los productos que tengan en su nombre la cadaena enchufe inteligente
    df.loc[df['Nombre del Producto'].str.contains('enchufe inteligente', case=False), 'Categoria'] = 'Asistente Virtual'

    # A esos valores cambiar su categoria a 'TV Box'
    df.loc[(df['Nombre del Producto'].str.contains('tv', case=False)) & (
        ~df['Categoria'].str.contains('Herramienta|Soporte|Cable|Teclado|Parlante')), 'Categoria'] = 'TV Box'

    # cambiar categoria a 'Salud' para los productos que tengan en su nombre la cadena sillon
    df.loc[df['Nombre del Producto'].str.contains('sillon', case=False), 'Categoria'] = 'Salud'

    # cambiar categoria a 'Electrodomesticos' para los productos que tengan en su nombre la cadena tostadora
    df.loc[df['Nombre del Producto'].str.contains('tostadora', case=False), 'Categoria'] = 'Electrodomesticos'

    # cambiar categoria a 'Conectividad' para los productos que tengan en su nombre la cadena wifi y qeu su categoria sea distinta de : Herramienta, Asistente Virtual, Luces, Adaptador y Router
    df.loc[(df['Nombre del Producto'].str.contains('wifi', case=False)) & (~df['Categoria'].str.contains(
        'Herramienta|Asistente Virtual|Luces|Adaptador|Router')), 'Categoria'] = 'Conectividad'

    # cambiar categoria a 'Conectividad' para los productos que tengan en su nombre la cadena adaptador inalambrico wifi
    df.loc[
        df['Nombre del Producto'].str.contains('adaptador inalambrico wifi', case=False), 'Categoria'] = 'Conectividad'

    # cambiar su cateogira a 'Conectividad'
    df.loc[df['Categoria'] == 'Extensor Wifi', 'Categoria'] = 'Conectividad'

    # cambiar su categoria a 'Herramienta' para los productos que tengan en su nombre la cadena apollo
    df.loc[df['Nombre del Producto'].str.contains('apollo', case=False), 'Categoria'] = 'Herramienta'

    # cambiar la cattegoria de ese producto a Accesorio Gamer
    df.loc[df['Nombre del Producto'].str.contains('cubre', case=False), 'Categoria'] = 'Accesorio Gamer'

    # cambiar la categoria de esos productos a 'Electricidad'
    df.loc[df['Nombre del Producto'].str.contains('zapatilla', case=False), 'Categoria'] = 'Electricidad'

    # cambiar la categoria a Herramienta para los productos que tengan las siguientes cadenas: mascara uv
    df.loc[df['Nombre del Producto'].str.contains('mascara uv', case=False), 'Categoria'] = 'Herramienta'

    # Cambiar la categoria de los produtos con la cadena balanza
    df.loc[df['Nombre del Producto'].str.contains('balanza', case=False), 'Categoria'] = 'Accesorio'

    # cambiar categoria a 'Adaptador' de los productos que tengan en su cadena placa de sonido
    df.loc[df['Nombre del Producto'].str.contains('placa de sonido', case=False), 'Categoria'] = 'Adaptador'

    # cambiar categoria a 'Conectividad' de los productos que tengan en su caden placa de red
    df.loc[df['Nombre del Producto'].str.contains('placa de red', case=False), 'Categoria'] = 'Conectividad'

    # cambiar la categoria de ese producto a 'Disco externo'
    df.loc[df['Nombre del Producto'].str.contains('carcasa', case=False), 'Categoria'] = 'Disco Externo'

    # A este valor cmabiar su categoria a Discos Externo
    df.loc[df['Nombre del Producto'].str.contains('disco', case=False) & df['Nombre del Producto'].str.contains('externo',case=False) & ~df['Nombre del Producto'].str.contains('cable', case=False), 'Categoria'] = 'Disco Externo'

    # de estos valores cambiar su categoria a 'Cable' si ademas no tienen la cadena Carcasa
    df.loc[df['Nombre del Producto'].str.contains('disco', case=False) & df['Nombre del Producto'].str.contains('cable',case=False) & ~df['Nombre del Producto'].str.contains('carcasa', case=False), 'Categoria'] = 'Cable'

    # de esos valores cambiar su categoria a 'Servicio Tecnico'
    df.loc[df['Nombre del Producto'].str.contains('disco', case=False) & df['Nombre del Producto'].str.contains(
        'servicio tecnico', case=False), 'Categoria'] = 'Servicio Tecnico'

    # cambiar ese producto  a 'Juguete'
    df.loc[df['Nombre del Producto'].str.contains('ajedrez', case=False), 'Categoria'] = 'Juguete'

    # cambiar la categoria de ese producto a 'Disco Externo'
    df.loc[df['Nombre del Producto'].str.contains('hd externo', case=False), 'Categoria'] = 'Disco Externo'

    # cambiar la categoria de ese producto a 'Accesorio'
    df.loc[df['Nombre del Producto'].str.contains('bluetooth auto', case=False), 'Categoria'] = 'Accesorio'

    # cambiar la categoria de ese producto a 'Cargador'
    df.loc[df['Nombre del Producto'].str.contains('cabezal', case=False), 'Categoria'] = 'Cargador'

    # cambiar la categoria de esos productos a 'Conectividad'
    df.loc[df['Nombre del Producto'].str.contains('^antena', case=False), 'Categoria'] = 'Conectividad'

    # cambiar la categoria de los productos con la cadena seña por arreglo a Servicio Tecnico
    df.loc[df['Nombre del Producto'].str.contains('seña por arreglo', case=False), 'Categoria'] = 'Servicio Tecnico'

    # a esos valores filtrados, cambiar la columna categoria a Kit Teclado & Mouse si su categoria es diferente de los siguientes valores: "Mouse", "Mousepad" o "Teclado"
    df.loc[df['Nombre del Producto'].str.contains('teclado|mouse', case=False), 'Categoria'] = df['Categoria'].apply(
        lambda x: 'Kit Teclado & Mouse' if x not in ['Mouse', 'Mousepad', 'Teclado'] else x)

    # al producto con el nombre conversor gamer cambiar su categoria a Conversor
    df.loc[df['Nombre del Producto'].str.contains('conversor', case=False), 'Categoria'] = 'Conversor'

    # a los valores con la cadena case disco 3.0 cambiar su categoria a Disco Interno
    df.loc[df['Nombre del Producto'].str.contains('case disco 3.0', case=False), 'Categoria'] = 'Disco Interno'

    # cambiar la categoria de esos productos a 'Herramienta'
    df.loc[df['Nombre del Producto'].str.contains('soplador', case=False), 'Categoria'] = 'Herramienta'

    # cambiar la categoria de esos productos a 'Accesorio'
    df.loc[df['Nombre del Producto'].str.contains('baston', case=False), 'Categoria'] = 'Accesorio'

    # en los productos donde este la cadena "ESTAÃ‘O",reemplazar esa cadena por "ESTAÑO"
    df['Nombre del Producto'] = df['Nombre del Producto'].str.replace('ESTAÃ‘O', 'ESTAÑO')

    # cambiar la categoria de los productos que tengan la cadena "ESTAÑO" a "Herramienta"
    df.loc[df['Nombre del Producto'].str.contains('ESTAÑO', case=False), 'Categoria'] = 'Herramienta'

    # cambiar la categoria de ese producto a Silla
    df.loc[df['Nombre del Producto'].str.contains('silla', case=False), 'Categoria'] = 'Silla'

    # cambiar la categoria a Herramienta en nombre de producto que tengan la cadena aire comprimido
    df.loc[df['Nombre del Producto'].str.contains('aire comprimido', case=False), 'Categoria'] = 'Herramienta'

    # cambiar la categoria de estos productos a Accesorio disco
    df.loc[df['Nombre del Producto'].str.contains('case carry', case=False), 'Categoria'] = 'Accesorio Disco'

    # cambiar la cateogira a accesorio a los productos que tengan la cadena "funda soporte para celular"
    df.loc[df['Nombre del Producto'].str.contains('funda soporte para celular', case=False), 'Categoria'] = 'Accesorio'

    # cambiar a categoria Conector el producto conector fpc, ignorar mayusculas y minusculas
    df.loc[df['Nombre del Producto'].str.contains('conector fpc', case=False), 'Categoria'] = 'Conector'

    # a las categorias que sean igual a "Flex" cambiarlas a "Contector"
    df.loc[df['Categoria'] == 'Flex', 'Categoria'] = 'Conector'

    # cambiar la categoria de productos con la cadena control remoto a Control Remoto
    df.loc[df['Nombre del Producto'].str.contains('control remoto', case=False), 'Categoria'] = 'Control Remoto'

    # cambiar este producto a Lente Camara
    df.loc[df['Nombre del Producto'].str.contains('lentes de camara', case=False), 'Categoria'] = 'Lente Camara'

    # cambiar la categoria del producto "lentes de realidad" virtual a Accesorio
    df.loc[df['Nombre del Producto'].str.contains('lentes de realidad virtual', case=False), 'Categoria'] = 'Accesorio'

    # cambiar la categoria del producto screen cleaner a Herramienta
    df.loc[df['Nombre del Producto'].str.contains('screen cleaner', case=False), 'Categoria'] = 'Herramienta'

    # cambiar la categoria a Tarjeta de Memoria para los productos que tengan en su nombre la cadena micro sd
    df.loc[df['Nombre del Producto'].str.contains('micro sd', case=False), 'Categoria'] = 'Tarjeta de Memoria'

    # cambiar la categoria a Accesorio a los productos que tengan la cadena licuadora
    df.loc[df['Nombre del Producto'].str.contains('licuadora', case=False), 'Categoria'] = 'Accesorio'

    # cambiar la cadena "PAÃ‘OS" por "PAÑOS"
    df['Nombre del Producto'] = df['Nombre del Producto'].str.replace('PAÃ‘OS', 'PAÑOS')

    # cambiar a categoria Herramienta los productos con la cadena paño
    df.loc[df['Nombre del Producto'].str.contains('paño', case=False), 'Categoria'] = 'Herramienta'

    # cambiar las cadenas MUÃ‘EQUERA a MUÑEQUERA
    df['Nombre del Producto'] = df['Nombre del Producto'].str.replace('MUÃ‘EQUERA', 'MUÑEQUERA')

    # cambiar la categoria a Accesorio a los productos que tengan la cadena "muñequera"
    df.loc[df['Nombre del Producto'].str.contains('muñequera', case=False), 'Categoria'] = 'Accesorio'

    # a los productos que cumplen las siguientes dos condiciones, si tiene la cadena "adaptador" y su categoria es "tv box" o "convertidor smart", cambiar sus categoria a adaptador
    df.loc[(df['Nombre del Producto'].str.contains('adaptador', case=False)) & (
        df['Categoria'].isin(['TV Box', 'Convertidor Smart'])), 'Categoria'] = 'Adaptador'

    # cambiar la categoria  a Convertidor Smart TV para los productos con la cadena roku
    df.loc[df['Nombre del Producto'].str.contains('roku', case=False), 'Categoria'] = 'Convertidor Smart TV'

    # ambiar la cateogira impresora para los productos que tengan la cadena tinta
    df.loc[df['Nombre del Producto'].str.contains('tinta', case=False), 'Categoria'] = 'Impresora'

    # ver valores con la cadena sin cateogira
    df.loc[df['Categoria'] == 'Sin Categoria']

    # cambiar a Pin Carga los productos que tengan la cadena pin carga
    df.loc[df['Nombre del Producto'].str.contains('pin carga', case=False), 'Categoria'] = 'Pin Carga'

    # cambiar la cateogira del producto SALDO A FAVOR PLACA REDMI NOTE 11 a Servicio Tecnico
    df.loc[df['Nombre del Producto'] == 'SALDO A FAVOR PLACA REDMI NOTE 11', 'Categoria'] = 'Servicio Tecnico'

    # a los valoes que tengan la cadena switch tp-link cambiar su categoria a Conectividad
    df.loc[df['Nombre del Producto'].str.contains('switch tp-link', case=False), 'Categoria'] = 'Conectividad'

    # cambiar las cadenas PortÃ¡til a Portatil
    df['Nombre del Producto'] = df['Nombre del Producto'].str.replace('PortÃ¡til', 'Portatil')

    # cambiar la categoria a Ventilador a los productos que tengan la cadena ventilador
    df.loc[df['Nombre del Producto'].str.contains('ventilador', case=False), 'Categoria'] = 'Ventilador'

    # cambiar la categoria a Camara Web a los productos que tengan la cadena webcam o web cam
    df.loc[df['Nombre del Producto'].str.contains('webcam|web cam', case=False), 'Categoria'] = 'Camara Web'

    # modificar la categoria de esa fila como : Sin Categoria
    df.loc[df['Cantidad'] == df['Cantidad'].max(), 'Categoria'] = 'Sin Categoria'

    # eliminar estas filas
    df = df.dropna(subset=['Cantidad'])

    # pasar a Int la columna Cantidad
    df['Cantidad'] = df['Cantidad'].astype(int)

    # redondear la columna ganancia a 0 decimales y transformar a int
    df['Ganancia'] = df['Ganancia'].round(0)
    df['Ganancia'] = df['Ganancia'].astype(int)

    # cambiar la cateogira de ese producto a Repuesto Parlante
    df.loc[df['Nombre del Producto'].str.contains('parlante', case=False) & df['Nombre del Producto'].str.contains(
        'buzzer', case=False), 'Categoria'] = 'Repuesto Parlante Celular'

    # Definir nuevo nombre con espacio antes de "FINAL"
    nuevo_nombre = f"{nombre_base} FINAL.csv"

    # Crear carpeta 'archive_final' si no existe
    os.makedirs(carpeta_destino, exist_ok=True)

    # Guardar en 'archive_final'
    ruta_destino = os.path.join(carpeta_destino, nuevo_nombre)
    df.to_csv(ruta_destino, index=False)
    print(f"\nArchivo guardado en '{carpeta_destino}': {nuevo_nombre}")

except Exception as e:
    print(f"Error: {str(e)}")