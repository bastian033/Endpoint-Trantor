from pymongo import MongoClient
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Configuración
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "DatosEmpresas"
POSTGRES_DB = "datosnormalizados"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "trantor2025"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
LOTE_SIZE = 1000  # Cantidad de documentos por lote

# Conexiones
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB]

pg_conn = psycopg2.connect(
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host=POSTGRES_HOST,
    port=POSTGRES_PORT
)
pg_cursor = pg_conn.cursor()

# Migrar cada colección
colecciones = mongo_db.list_collection_names()
for col_name in colecciones:
    print(f"⏳ Procesando colección: {col_name}")
    cursor = mongo_db[col_name].find({}, batch_size=LOTE_SIZE)
    first_batch = []

    try:
        for i, doc in enumerate(cursor):
            doc.pop('_id', None)
            first_batch.append(doc)

            if i == 0:
                # Crear tabla solo una vez con base en el primer documento
                columnas = list(doc.keys())
                columnas_sql = ", ".join([f'"{col}" TEXT' for col in columnas])
                pg_cursor.execute(f'DROP TABLE IF EXISTS "{col_name}";')
                pg_cursor.execute(f'CREATE TABLE "{col_name}" ({columnas_sql});')
                pg_conn.commit()

            # Cuando llegamos al tamaño del lote, insertamos
            if len(first_batch) >= LOTE_SIZE:
                df = pd.DataFrame(first_batch).fillna("").astype(str)
                tuples = [tuple(row) for row in df.values]
                cols_str = ', '.join([f'"{col}"' for col in df.columns])
                sql = f'INSERT INTO "{col_name}" ({cols_str}) VALUES %s'
                execute_values(pg_cursor, sql, tuples)
                pg_conn.commit()
                first_batch = []

        # Insertar lo que queda si hay registros pendientes
        if first_batch:
            df = pd.DataFrame(first_batch).fillna("").astype(str)
            tuples = [tuple(row) for row in df.values]
            cols_str = ', '.join([f'"{col}"' for col in df.columns])
            sql = f'INSERT INTO "{col_name}" ({cols_str}) VALUES %s'
            execute_values(pg_cursor, sql, tuples)
            pg_conn.commit()

        print(f"✔ Colección '{col_name}' migrada exitosamente.")
    except Exception as e:
        print(f"❌ Error al migrar colección '{col_name}': {e}")

# Cierre
pg_cursor.close()
pg_conn.close()
mongo_client.close()
