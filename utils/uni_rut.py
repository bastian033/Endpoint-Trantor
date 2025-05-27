from pymongo import MongoClient, UpdateOne

conexion = MongoClient("mongodb://localhost:27018")
db = conexion["DatosEmpresas"]

colecciones_especificas = [
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20052009",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20102014",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20152019",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20202023"
]

BATCH_SIZE = 1000

for nombre_col in colecciones_especificas:
    print(f"Procesando colección: {nombre_col}")
    col = db[nombre_col]
    batch = []
    procesados = 0
    total = col.count_documents({})
    for doc in col.find({}, {"_id": 1, "RUT": 1, "DV": 1}):
        rut = doc.get("RUT")
        dv = doc.get("DV")
        if rut is None or dv is None:
            continue
        rut_str = str(rut).lstrip("0")
        rut_unificado = f"{rut_str}-{str(dv).upper()}"
        batch.append(
            UpdateOne({"_id": doc["_id"]}, {"$set": {"rut_unificado": rut_unificado}})
        )
        procesados += 1
        if len(batch) >= BATCH_SIZE:
            col.bulk_write(batch, ordered=False)
            print(f"Procesados: {procesados}/{total}")
            batch = []
    if batch:
        col.bulk_write(batch, ordered=False)
        print(f"Procesados: {procesados}/{total}")
    # Crea el índice al final
    col.create_index("rut_unificado")
    print(f"Índice creado en 'rut_unificado' para {nombre_col}")

print("Campo 'rut_unificado' creado y indexado en todas las colecciones.")