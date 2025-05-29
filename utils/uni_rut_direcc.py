from pymongo import MongoClient, UpdateOne

conexion = MongoClient("mongodb://localhost:27017")
db = conexion["DatosEmpresas"]
coleccion = db.PUB_NOMBRES_PJ

BATCH_SIZE = 1000
batch = []
total = 0

print("Iniciando actualización de rut_completo...")

cursor = coleccion.find({"RUT": {"$exists": True}, "DV": {"$exists": True}}, {"RUT": 1, "DV": 1})
for doc in cursor:
    rut = str(doc["RUT"])
    dv = str(doc["DV"])
    rut_completo = f"{rut}-{dv}"
    batch.append(
        UpdateOne(
            {"_id": doc["_id"]},
            {"$set": {"rut_completo": rut_completo}}
        )
    )
    total += 1
    if len(batch) >= BATCH_SIZE:
        coleccion.bulk_write(batch)
        print(f"{total} documentos actualizados...")
        batch = []

if batch:
    coleccion.bulk_write(batch)
    print(f"{total} documentos actualizados...")

print("Proceso finalizado  total documentos procesados:", total)