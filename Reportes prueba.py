#%%
#ver valores unicos en la columna categoria
df['Categoria'].unique()
#%%
#en los productos donde este la cadena "ESTAÃ‘O",reemplazar esa cadena por "ESTAÑO"
df['Nombre del Producto'] = df['Nombre del Producto'].str.replace('ESTAÃ‘O', 'ESTAÑO')
#%%
#cambiar la categoria de los productos que tengan la cadena "ESTAÑO" a "Herramienta"
df.loc[df['Nombre del Producto'].str.contains('ESTAÑO', case=False), 'Categoria'] = 'Herramienta'
#%%
#cambiar la categoria de ese producto a Silla
df.loc[df['Nombre del Producto'].str.contains('silla', case=False), 'Categoria'] = 'Silla'
#%%
#cambiar la categoria a Herramienta en nombre de producto que tengan la cadena aire comprimido
df.loc[df['Nombre del Producto'].str.contains('aire comprimido', case=False), 'Categoria'] = 'Herramienta'
#%%
#cambiar la categoria de estos productos a Accesorio disco
df.loc[df['Nombre del Producto'].str.contains('case carry', case=False), 'Categoria'] = 'Accesorio Disco'
#%%
#cambiar la cateogira a accesorio a los productos que tengan la cadena "funda soporte para celular"
df.loc[df['Nombre del Producto'].str.contains('funda soporte para celular', case=False), 'Categoria'] = 'Accesorio'
#%%
#cambiar a categoria Conector el producto conector fpc, ignorar mayusculas y minusculas
df.loc[df['Nombre del Producto'].str.contains('conector fpc', case=False), 'Categoria'] = 'Conector'
#%%
#a las categorias que sean igual a "Flex" cambiarlas a "Contector"
df.loc[df['Categoria'] == 'Flex', 'Categoria'] = 'Conector'
#%%
#cambiar la categoria de productos con la cadena control remoto a Control Remoto
df.loc[df['Nombre del Producto'].str.contains('control remoto', case=False), 'Categoria'] = 'Control Remoto'
#%%
#cambiar este producto a Lente Camara
df.loc[df['Nombre del Producto'].str.contains('lentes de camara', case=False), 'Categoria'] = 'Lente Camara'
#%%
#cambiar la categoria del producto "lentes de realidad" virtual a Accesorio
df.loc[df['Nombre del Producto'].str.contains('lentes de realidad virtual', case=False), 'Categoria'] = 'Accesorio'
#%%
#cambiar la categoria del producto screen cleaner a Herramienta
df.loc[df['Nombre del Producto'].str.contains('screen cleaner', case=False), 'Categoria'] = 'Herramienta'
#%%
#cambiar la categoria a Tarjeta de Memoria para los productos que tengan en su nombre la cadena micro sd
df.loc[df['Nombre del Producto'].str.contains('micro sd', case=False), 'Categoria'] = 'Tarjeta de Memoria'
#%%
#cambiar la categoria a Accesorio a los productos que tengan la cadena licuadora
df.loc[df['Nombre del Producto'].str.contains('licuadora', case=False), 'Categoria'] = 'Accesorio'
#%%
#cambiar la cadena "PAÃ‘OS" por "PAÑOS"
df['Nombre del Producto'] = df['Nombre del Producto'].str.replace('PAÃ‘OS', 'PAÑOS')
#%%
#cambiar a categoria Herramienta los productos con la cadena paño
df.loc[df['Nombre del Producto'].str.contains('paño', case=False), 'Categoria'] = 'Herramienta'
#%%
#cambiar las cadenas MUÃ‘EQUERA a MUÑEQUERA
df['Nombre del Producto'] = df['Nombre del Producto'].str.replace('MUÃ‘EQUERA', 'MUÑEQUERA')
#%%
#cambiar la categoria a Accesorio a los productos que tengan la cadena "muñequera"
df.loc[df['Nombre del Producto'].str.contains('muñequera', case=False), 'Categoria'] = 'Accesorio'
#%%
#a los productos que cumplen las siguientes dos condiciones, si tiene la cadena "adaptador" y su categoria es "tv box" o "convertidor smart", cambiar sus categoria a adaptador
df.loc[(df['Nombre del Producto'].str.contains('adaptador', case=False)) & (df['Categoria'].isin(['TV Box', 'Convertidor Smart'])), 'Categoria'] = 'Adaptador'
#%%
 #cambiar la categoria  a Convertidor Smart TV para los productos con la cadena roku
df.loc[df['Nombre del Producto'].str.contains('roku', case=False), 'Categoria'] = 'Convertidor Smart TV'
#%%
#ambiar la cateogira impresora para los productos que tengan la cadena tinta
df.loc[df['Nombre del Producto'].str.contains('tinta', case=False), 'Categoria'] = 'Impresora'
#%%
#ver valores con la cadena sin cateogira
df.loc[df['Categoria'] == 'Sin Categoria']
#%%
#cambiar a Pin Carga los productos que tengan la cadena pin carga
df.loc[df['Nombre del Producto'].str.contains('pin carga', case=False), 'Categoria'] = 'Pin Carga'
#%%
#cambiar la cateogira del producto SALDO A FAVOR PLACA REDMI NOTE 11 a Servicio Tecnico
df.loc[df['Nombre del Producto'] == 'SALDO A FAVOR PLACA REDMI NOTE 11', 'Categoria'] = 'Servicio Tecnico'
#%%
#a los valoes que tengan la cadena switch tp-link cambiar su categoria a Conectividad
df.loc[df['Nombre del Producto'].str.contains('switch tp-link', case=False), 'Categoria'] = 'Conectividad'
#%%
#cambiar las cadenas PortÃ¡til a Portatil
df['Nombre del Producto'] = df['Nombre del Producto'].str.replace('PortÃ¡til', 'Portatil')
#%%
#cambiar la categoria a Ventilador a los productos que tengan la cadena ventilador
df.loc[df['Nombre del Producto'].str.contains('ventilador', case=False), 'Categoria'] = 'Ventilador'
#%%
#cambiar la categoria a Camara Web a los productos que tengan la cadena webcam o web cam
df.loc[df['Nombre del Producto'].str.contains('webcam|web cam', case=False), 'Categoria'] = 'Camara Web'