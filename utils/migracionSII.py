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

# Acumulador por RUT
empresas_por_rut = {}

def extraer_anios_desde_nombre(nombre_coleccion):
    try:
        partes = nombre_coleccion.split("_")[-1]  # Ej: "20202023"
        inicio = int(partes[:4])
        fin = int(partes[4:])
        return list(range(inicio, fin + 1))
    except:
        return []

for nombre_coleccion in colecciones_especificas:
    print(f"Migrando colección: {nombre_coleccion}")
    coleccion = db_origen[nombre_coleccion]
    años_extraidos = extraer_anios_desde_nombre(nombre_coleccion)

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

        # Lista de años comerciales para este documento
        años_comerciales = [{"año": año, "origen": nombre_coleccion} for año in años_extraidos]

        if rut not in empresas_por_rut:
            empresas_por_rut[rut] = {
                "rut": rut,
                "tags": [],
                "fecha_subida_datos": fecha_guardado,
                "años_comerciales": años_comerciales,
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
        else:
            # Fusionar años comerciales sin duplicados
            ya = empresas_por_rut[rut]["años_comerciales"]
            ya_set = {(a["año"], a["origen"]) for a in ya}
            for ac in años_comerciales:
                if (ac["año"], ac["origen"]) not in ya_set:
                    ya.append(ac)
                    ya_set.add((ac["año"], ac["origen"]))

batch = []
for empresa in empresas_por_rut.values():
    batch.append(InsertOne(empresa))
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