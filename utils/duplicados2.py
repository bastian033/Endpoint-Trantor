from pymongo import MongoClient, InsertOne
from collections import defaultdict
import math

conexion = MongoClient("mongodb://localhost:27017")
db = conexion["DatosNormalizados"]
coleccion = db.empresas

BATCH_SIZE = 500
batch = []

def obtener_ruts():
    ultimo = None
    count = 0
    print("Obteniendo RUTs únicos...")
    while True:
        filtro = {"rut": {"$gt": ultimo}} if ultimo else {}
        doc = coleccion.find(filtro, {"rut": 1}).sort("rut", 1).limit(1)
        doc = list(doc)
        if not doc:
            break
        rut = doc[0]["rut"]
        yield rut
        ultimo = rut
        count += 1
        if count % 1000 == 0:
            print(f"  {count} RUTs leídos...")

def normalizar_fecha(fecha):
    if fecha is None:
        return ""
    if isinstance(fecha, float) and math.isnan(fecha):
        return ""
    return str(fecha)

def fusionar_personas(arrays, campo_rut="rut", campo_fecha="fecha_actualizacion"):
    todos = []
    for arr in arrays:
        if arr:
            todos.extend(arr)
    agrupados = defaultdict(list)
    for item in todos:
        clave = item.get(campo_rut)
        if clave:
            agrupados[clave].append(item)
    resultado = []
    for items in agrupados.values():
        items_ordenados = sorted(
            items,
            key=lambda x: normalizar_fecha(x.get(campo_fecha)),
            reverse=True
        )
        for i, item in enumerate(items_ordenados):
            item = dict(item)
            item["vigente"] = (i == 0)
            resultado.append(item)
    return resultado

def fusionar_unico_vigente(arrays, campo_fecha="fecha_actualizacion"):
    todos = []
    for arr in arrays:
        if arr:
            todos.extend(arr)
    if not todos:
        return []
    items_ordenados = sorted(
        todos,
        key=lambda x: normalizar_fecha(x.get(campo_fecha)),
        reverse=True
    )
    resultado = []
    for i, item in enumerate(items_ordenados):
        item = dict(item)
        item["vigente"] = (i == 0)
        resultado.append(item)
    return resultado

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
            key=lambda x: normalizar_fecha(x.get(campo_fecha)),
            reverse=True
        )
        for i, item in enumerate(items_ordenados):
            item = dict(item)
            resultado.append(item)
    return resultado

print("Iniciando proceso de fusión y limpieza de duplicados...\n")
total_ruts = 0
for rut in obtener_ruts():
    total_ruts += 1
    print(f"Procesando RUT: {rut} ({total_ruts})")
    docs = list(coleccion.find({"rut": rut}))
    if not docs:
        continue

    base = docs[0]
    historia = base.get("historia", {})

    # Socios y representantes_legales: solo uno vigente por persona (por rut)
    for campo in ["socios", "representantes_legales"]:
        arrays = [doc.get("historia", {}).get(campo, []) for doc in docs]
        historia[campo] = fusionar_personas(arrays)

    # razon_social y actividades_economicas: solo uno vigente por empresa
    for campo in ["razon_social", "actividades_economicas"]:
        arrays = [doc.get("historia", {}).get(campo, []) for doc in docs]
        historia[campo] = fusionar_unico_vigente(arrays)

    # direcciones y actuacion: puede haber varias vigentes
    for campo in ["direcciones", "actuacion"]:
        arrays = [doc.get("historia", {}).get(campo, []) for doc in docs]
        historia[campo] = fusionar_array(arrays)

    base["historia"] = historia

    # Fusionar años_comerciales
    anios = set()
    for doc in docs:
        for ac in doc.get("años_comerciales", []):
            anios.add((ac.get("año"), ac.get("origen")))
    base["años_comerciales"] = [{"año": a, "origen": o} for a, o in sorted(anios)]

    batch.append(InsertOne(base))
    coleccion.delete_many({"rut": rut})
    if len(batch) >= BATCH_SIZE:
        coleccion.bulk_write(batch)
        print(f"  Batch insertado ({len(batch)} documentos).")
        batch = []

if batch:
    coleccion.bulk_write(batch)
    print(f"  Batch final insertado ({len(batch)} documentos).")

print("\nProceso finalizado. Total de RUTs procesados:", total_ruts)