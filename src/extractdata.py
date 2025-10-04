import pandas as pd
from sqlalchemy import create_engine
import os

# --- 1. Definir la Conexión ---
# Formato: 'dialecto+driver://usuario:contraseña@host:puerto/base_de_datos'
usuario = 'root'
contraseña = '' 
host = 'localhost'
nombre_bd = 'coffeeshop' 

cadena_conexion = f'mysql+pymysql://{usuario}:{contraseña}@{host}/{nombre_bd}'

try:
    engine = create_engine(cadena_conexion)
    print("✅ Conexión con el Engine de SQLAlchemy establecida.")
except Exception as e:
    print(f"❌ Error al conectar: Asegúrate que MariaDB de XAMPP esté corriendo y la BD exista. Error: {e}")
    # Puedes salir del script si la conexión falla
    exit()

# --- 2. Preparar el Bucle y la Exportación ---
# C:\projects\coffeeshop\csv\menu_items
# C:\projects\coffeeshop\csv\payment_methods
# C:\projects\coffeeshop\csv\stores
# C:\projects\coffeeshop\csv\\transaction_items
# C:\projects\coffeeshop\csv\\transactions
# C:\projects\coffeeshop\csv\\users
# C:\projects\coffeeshop\csv\\vouchers
carpeta_csv = 'C:\projects\coffeeshop\csv\\vouchers'
archivos_csv = [f for f in os.listdir(carpeta_csv) if f.endswith('.csv')]

for nombre_archivo in archivos_csv:
    ruta_completa = os.path.join(carpeta_csv, nombre_archivo)
    
    # El nombre de la tabla será el nombre del archivo (ej. 'datos_clientes')
    nombre_tabla = os.path.splitext(nombre_archivo)[0]
    
    print(f"\nProcesando archivo: {nombre_archivo} -> Tabla: {nombre_tabla}")
    
    try:
        # Lee el CSV
        df = pd.read_csv(ruta_completa)
        
        # Exporta a MariaDB usando el método to_sql() de Pandas
        df.to_sql(
            name=nombre_tabla,      # El nombre de la tabla a crear/reemplazar
            con=engine,             # El Engine de SQLAlchemy
            if_exists='replace',    # 'replace' (sobrescribe) o 'append' (añade)
            index=False             # No incluir la columna de índice de Pandas
        )
        
        print(f"✅ ¡Exportación completada a la tabla '{nombre_tabla}'!")
        
    except Exception as e:
        print(f"❌ Error al exportar {nombre_archivo}. Error: {e}")

# Cierra el Engine al finalizar
engine.dispose()
