from pymongo import MongoClient, InsertOne
from collections import defaultdict

conexion = MongoClient("mongodb://localhost:27018")
db = conexion["DatosNormalizados"]

coleccion = db.empresas

ruts = coleccion.distinct("rut")
BATCH_SIZE = 500
batch = []

def fusionar_array(arrays, campo_fecha="fecha_actualizacion"):
    todos = []
    for arr in arrays:
        if arr:
            todos.extend(arr)
    agrupados = defaultdict(list)

    for item in todos:
        clave = tuple((k, v) for k, v in item.items() if k not in ["vigente", campo_fecha])
        agrupados[clave].append(item)
    resultado = []

    for items in agrupados.values():
        items_ordenados = sorted(
            items,
            key=lambda x: x.get(campo_fecha) or "",
            reverse=True
        )

        for i, item in enumerate(items_ordenados):
            item = dict(item)  
            item["vigente"] = (i == 0)  
            resultado.append(item)
    return resultado

for rut in ruts:
    docs = list(coleccion.find({"rut": rut}))
    if not docs:
        continue

    base = docs[0]
    historia = base.get("historia", {})

    for campo in ["socios", "representantes_legales", "direcciones", "razon_social", "actividades_economicas", "actuacion"]:
        arrays = [doc.get("historia", {}).get(campo, []) for doc in docs]
        historia[campo] = fusionar_array(arrays)
    base["historia"] = historia
    anios = set()

    for doc in docs:
        for ac in doc.get("años_comerciales", []):
            anios.add((ac.get("año"), ac.get("origen")))
    base["años_comerciales"] = [{"año": a, "origen": o} for a, o in sorted(anios)]
    batch.append(InsertOne(base))
    coleccion.delete_many({"rut": rut})
    if len(batch) >= BATCH_SIZE:
        coleccion.insert_many(batch)
        batch = []

if batch:
    coleccion.insert_many(batch)

print("Listo")