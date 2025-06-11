from pymongo import MongoClient, UpdateOne
from datetime import datetime
from db_origen import conexion_base_datos_origen
from db import conexion_base_datos as conexion_base_datos_destino

conexion = MongoClient("mongodb://localhost:27018")
db_origen = conexion["DatosEmpresas"]
db_destino = conexion["DatosNormalizados"]
fecha_guardado = datetime.now().date().isoformat()

colecciones = [f"DatosGob{anio}" for anio in range(2013, 2025)]
print(f"Total de colecciones: {colecciones}")

BATCH_SIZE = 1000

for nombre_coleccion in colecciones:
    print(f"Migrando colección: {nombre_coleccion}")
    coleccion = db_origen[nombre_coleccion]
    cursor = coleccion.find(batch_size=BATCH_SIZE)
    operaciones = []
    for doc in cursor:
        rut = doc.get("RUT", None)
        if not rut:
            continue

        nueva_direccion = {
            "vigente": True,
            "fecha_actualizacion": doc.get("Fecha de aprobacion x SII", fecha_guardado),
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

        nuevo_doc = {
            "rut": rut,
            "tags": [],
            "fecha_subida_datos": fecha_guardado,
            "historia": {
                "socios": [],
                "representantes_legales": [],
                "direcciones": [nueva_direccion],
                "razon_social": [
                    {
                        "razon_social": doc.get("Razon Social", None),
                        "codigo_sociedad": doc.get("Codigo de sociedad", None),
                        "fecha_actualizacion": doc.get("Fecha de aprobacion x SII", fecha_guardado),
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

        empresa = db_destino.empresas.find_one({"rut": rut})
        if empresa:
            direcciones = empresa.get("historia", {}).get("direcciones", [])
            direccion_vigente = next((d for d in direcciones if d.get("vigente")), None)
            if direccion_vigente:
                misma_direccion = (
                    direccion_vigente.get("comuna") == nueva_direccion["comuna"] and
                    direccion_vigente.get("region") == nueva_direccion["region"]
                )
                if not misma_direccion:
                    # Marca la anterior como no vigente y agrega la nueva
                    operaciones.append(
                        UpdateOne(
                            {"rut": rut, "historia.direcciones.vigente": True},
                            {
                                "$set": {"historia.direcciones.$.vigente": False,
                                         "historia.direcciones.$.fecha_termino": fecha_guardado}
                            }
                        )
                    )
                    operaciones.append(
                        UpdateOne(
                            {"rut": rut},
                            {"$push": {"historia.direcciones": nueva_direccion}}
                        )
                    )
            else:
                operaciones.append(
                    UpdateOne(
                        {"rut": rut},
                        {"$push": {"historia.direcciones": nueva_direccion}}
                    )
                )
        else:
            operaciones.append(
                UpdateOne(
                    {"rut": rut},
                    {"$set": nuevo_doc},
                    upsert=True
                )
            )

        if len(operaciones) >= BATCH_SIZE:
            db_destino.empresas.bulk_write(operaciones)
            operaciones = []
    if operaciones:
        db_destino.empresas.bulk_write(operaciones)

print("Migracion completada")
