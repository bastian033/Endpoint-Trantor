from pymongo import MongoClient
from datetime import datetime

conexion = MongoClient("mongodb://localhost:27018")
db_origen = conexion["DatosEmpresas"]
db_destino = conexion["DatosNormalizados"]
fecha_guardado = datetime.now().date().isoformat()

# Colecciones de origen
colecciones = [f"DatosGob{anio}" for anio in range(2013, 2025)]
print(f"Total de colecciones: {colecciones}")

BATCH_SIZE = 1000

for nombre_coleccion in colecciones:
    print(f"Migrando colección: {nombre_coleccion}")
    coleccion = db_origen[nombre_coleccion]
    batch = []
    cursor = coleccion.find(batch_size=BATCH_SIZE)
    for doc in cursor:
        nuevo_doc = {
            "rut": doc.get("RUT", None),
            "tags": [],
            "fecha_subida_datos" : fecha_guardado,
            "historia": {
                "socios": [],
                "representantes_legales": [],
                "direcciones": [
                    {
                        "vigente": True,
                        "fecha_actualizacion": doc.get("Fecha de aprobacion x SII", None),
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
            db_destino.empresas.insert_many(batch)
            batch = []
    if batch:
        db_destino.empresas.insert_many(batch)

print("Migración completada.")