from pymongo import MongoClient, InsertOne
from datetime import datetime

conexion = MongoClient("mongodb://localhost:27018")
db_origen = conexion["DatosEmpresas"]
db_destino = conexion["DatosNormalizados"]
fecha_guardado = datetime.now().date().isoformat()

colecciones_especificas = [
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20052009",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20102014",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20152019",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20202023"
]

BATCH_SIZE = 1000

for nombre_coleccion in colecciones_especificas:
    print(f"Migrando colección: {nombre_coleccion}")
    coleccion = db_origen[nombre_coleccion]
    batch = []
    for doc in coleccion.find():
        rut = f'{doc.get("RUT", "")}-{doc.get("DV", "")}'
        razon_social = doc.get("Razón social", None)
        codigo_sociedad = None
        fecha_inicio = doc.get("Fecha inicio de actividades vige", None)
        fecha_termino = doc.get("Fecha término de giro", None)
        actividad = doc.get("Actividad económica", None)
        rubro = doc.get("Rubro económico", None)
        subrubro = doc.get("Subrubro económico", None)
        comuna = doc.get("Comuna", None)
        region = doc.get("Región", None)
        fuente = doc.get("fuente", "SII")

        nuevo_doc = {
            "rut": rut,
            "tags": [],
            "fecha_subida_datos": fecha_guardado,
            "historia": {
                "socios": [],
                "representantes_legales": [],
                "direcciones": [
                    {
                        "vigente": True,
                        "fecha_actualizacion": fecha_inicio,
                        "tipo_direccion": None,
                        "calle": None,
                        "numero": None,
                        "bloque": None,
                        "departamento": None,
                        "villa_poblacion": None,
                        "ciudad": None,
                        "comuna": comuna,
                        "region": region,
                        "origen": "SII"
                    }
                ],
                "razon_social": [
                    {
                        "razon_social": razon_social,
                        "codigo_sociedad": codigo_sociedad,
                        "fecha_actualizacion": fecha_inicio,
                        "origen": "SII",
                        "vigente": True
                    }
                ],
                "actividades_economicas": [
                    {
                        "giro": None,
                        "actividad": actividad,
                        "codigo_sii": None,
                        "rubro": rubro,
                        "subrubro": subrubro,
                        "fecha_actualizacion": fecha_inicio,
                        "fecha_inicio_actividades": fecha_inicio,
                        "fecha_termino_actividades": fecha_termino,
                        "origen": "SII",
                        "vigente": True
                    }
                ],
                "actuacion": [
                    {
                        "tipo_actuacion": None,
                        "fecha_aprobacion_SII": None,
                        "descripcion": None,
                        "origen": "SII"
                    }
                ]
            }
        }
        batch.append(InsertOne(nuevo_doc))
        if len(batch) >= BATCH_SIZE:
            try:
                db_destino.empresas.bulk_write(batch, ordered=False)
            except Exception as e:
                print(f"Error en bulk_write: {e}")
            batch = []
    if batch:
        try:
            db_destino.empresas.bulk_write(batch, ordered=False)
        except Exception as e:
            print(f"Error en bulk_write: {e}")

print("Migración completada.")