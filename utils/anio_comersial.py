from pymongo import MongoClient, UpdateOne

conexion = MongoClient("mongodb://localhost:27017")
db_origen = conexion["DatosEmpresas"]
db_destino = conexion["DatosNormalizados"]

colecciones_especificas = [
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20052009",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20102014",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20152019",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20202023"
]

def extraer_anios_desde_nombre(nombre_coleccion):
    try:
        partes = nombre_coleccion.split("_")[-1]
        inicio = int(partes[:4])
        fin = int(partes[4:])
        return list(range(inicio, fin + 1))
    except:
        return []

BATCH_SIZE = 1000
batch = []
procesados = 0

filtro = {
    "$or": [
        {"años_comerciales": {"$exists": False}},
        {"años_comerciales": {"$size": 0}}
    ]
}
total = db_destino.empresas.count_documents(filtro)
print(f"Total de documentos a procesar: {total}")

for doc in db_destino.empresas.find(filtro, {"rut": 1}):
    rut = doc["rut"]
    if not rut or "-" not in rut:
        continue
    rut_unificado = rut.lstrip("0").upper()
    anios = set()
    for nombre_col in colecciones_especificas:
        col = db_origen[nombre_col]
        for origen_doc in col.find({"rut_unificado": rut_unificado}):
            for año in extraer_anios_desde_nombre(nombre_col):
                anios.add((año, nombre_col))
    array_anios = [{"año": año, "origen": origen} for año, origen in sorted(anios)]
    batch.append(
        UpdateOne({"_id": doc["_id"]}, {"$set": {"años_comerciales": array_anios}})
    )
    procesados += 1
    if procesados % 10000 == 0:
        print(f"Procesados: {procesados}/{total}")
    if len(batch) >= BATCH_SIZE:
        db_destino.empresas.bulk_write(batch, ordered=False)
        batch = []

if batch:
    db_destino.empresas.bulk_write(batch, ordered=False)

print(f"Proceso terminado. Total procesados: {procesados}")