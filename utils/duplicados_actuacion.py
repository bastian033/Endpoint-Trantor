from pymongo import MongoClient

conexion = MongoClient("mongodb://localhost:27017")
db_destino = conexion["DatosNormalizados"]
col_destino = db_destino.empresas

# Colecciones de origen
anios = range(2013, 2026)
colecciones_origen = [f"DatosGob{anio}" for anio in anios]
dbs_origen = [conexion[nombre] for nombre in colecciones_origen]

BATCH_SIZE = 200

def norm(x):
    v = str(x or "").strip().upper()
    return "" if v in ("NAN", "NONE", "NULL", "") else v

def key_actuacion(d):
    return (
        norm(d.get("tipo_actuacion")),
        norm(d.get("fecha_aprobacion_SII")),
        norm(d.get("descripcion")),
        norm(d.get("origen")),
    )

def mapear_actuacion(doc):
    return {
        "tipo_actuacion": doc.get("Tipo de actuacion") or doc.get("tipo_actuacion"),
        "fecha_aprobacion_SII": doc.get("Fecha de aprobacion x SII") or doc.get("fecha_aprobacion_SII"),
        "descripcion": doc.get("Descripcion") or doc.get("descripcion"),
        "origen": "DatosGob",
    }

def actuacion_vacia(d):
    campos = ["tipo_actuacion", "fecha_aprobacion_SII", "descripcion"]
    return all(norm(d.get(c)) == "" for c in campos)

procesados = 0

print("Iniciando deduplicación de actuaciones (DatosGob2013-2025)...")
total_empresas = col_destino.count_documents({})
print(f"Total de empresas a procesar: {total_empresas}")

cursor = col_destino.find({}, {"rut": 1, "historia.actuacion": 1})
batch = []
for empresa in cursor:
    rut = empresa.get("rut")
    if not rut:
        continue

    print(f"\nProcesando empresa RUT: {rut}")

    # 1. Trae actuaciones desde todas las colecciones de origen
    actuaciones_gob = []
    for db in dbs_origen:
        # Busca en todas las colecciones de la base de datos de origen
        for nombre_col in db.list_collection_names():
            col = db[nombre_col]
            actuaciones_gob.extend([mapear_actuacion(doc) for doc in col.find({"RUT": rut})])

    print(f"  Actuaciones encontradas en origen: {len(actuaciones_gob)}")

    # 2. Deduplicar por clave robusta
    grupos = {}
    for d in actuaciones_gob:
        clave = key_actuacion(d)
        if clave not in grupos:
            grupos[clave] = d

    actuaciones_gob_final = list(grupos.values())

    # 3. Preserva actuaciones históricas de SII (si existen en destino)
    actuaciones_destino = empresa.get("historia", {}).get("actuacion", [])
    actuaciones_sii = [
        a for a in actuaciones_destino
        if a.get("origen") == "SII"
        and key_actuacion(a) not in {key_actuacion(gob) for gob in actuaciones_gob_final}
    ]

    # 4. Unir y guardar
    actuaciones_final = actuaciones_sii + actuaciones_gob_final

    # Elimina actuaciones completamente vacías
    actuaciones_final = [a for a in actuaciones_final if not actuacion_vacia(a)]

    print(f"  Total actuaciones finales para empresa: {len(actuaciones_final)}")

    batch.append({
        "filter": {"_id": empresa["_id"]},
        "update": {"$set": {"historia.actuacion": actuaciones_final}}
    })
    procesados += 1

    if len(batch) >= BATCH_SIZE:
        for op in batch:
            col_destino.update_one(op["filter"], op["update"])
        print(f"\n{procesados} empresas procesadas hasta ahora...")
        batch = []

if batch:
    for op in batch:
        col_destino.update_one(op["filter"], op["update"])
    print(f"\n{procesados} empresas procesadas hasta ahora...")

print(f"\nEmpresas procesadas: {procesados}")
print("Deduplicación de actuaciones finalizada.")