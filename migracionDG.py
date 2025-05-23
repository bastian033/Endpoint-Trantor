# Script para migrar DatosGob

from pymongo import MongoClient
from bson import ObjectId

# Configuración
conexion = MongoClient("mongodb://localhost:27017")
db_origen = conexion["DatosEmpresas"]
db_destino = conexion["DatosNormalizados"]

# Colecciones de origen
colecciones = [f"DatosGob{anio}" for anio in range(2022, 2025)]
print(f"Total de colecciones: {colecciones}")

BATCH_SIZE = 1000

# Cambia por el último _id insertado exitosamente
ultimo_id = ObjectId('68011db39ccb37402aee8780')

for nombre_coleccion in colecciones:
    print(f"Migrando colección: {nombre_coleccion}")
    coleccion = db_origen[nombre_coleccion]
    batch = []
    cursor = coleccion.find({"_id": {"$gt": ultimo_id}}, batch_size=BATCH_SIZE)
    for doc in cursor:
        if db_destino.empresas.find_one({"rut": doc.get("RUT", None)}):
            continue
        nuevo_doc = {
            "rut": doc.get("RUT", None),
            "tags": [],
            "historia": {
                "socios": [],
                "representantes_legales": [],
                "direcciones": [
                    {
                        "vigente": True,
                        "fecha_actualizacion": None,
                        "tipo_direccion": None,
                        "calle": None,
                        "numero": None,
                        "bloque": None,
                        "departamento": None,
                        "villa_poblacion": None,
                        "ciudad": None,
                        "comuna": doc.get("Comuna Tributaria", None),
                        "region": doc.get("Region Tributaria", None),
                        "origen": "DatosGob"
                    }
                ],
                "razon_social": [
                    {
                        "razon_social": doc.get("Razon Social", None),
                        "codigo_sociedad": doc.get("Codigo de sociedad", None),
                        "fecha_actualizacion": doc.get("Fecha de aprobacion x SII", None),
                        "origen": "DatosGob",
                        "vigente": True
                    }
                ],
                "actividades_economicas": [],
                "actuacion": [
                    {
                        "tipo_actuacion": doc.get("Tipo de actuacion", None),
                        "fecha_aprobacion_SII": doc.get("Fecha de aprobacion x SII", None),
                        "origen": "DatosGob"
                    }
                ]
            }
        }
        batch.append(nuevo_doc)
        if len(batch) >= BATCH_SIZE:
            try:
                db_destino.empresas.insert_many(batch)
            except Exception as e:
                print(f"Error al insertar lote: {e}")
            batch = []
    if batch:
        try:
            db_destino.empresas.insert_many(batch)
        except Exception as e:
            print(f"Error al insertar lote final: {e}")

print("Migración completada.")