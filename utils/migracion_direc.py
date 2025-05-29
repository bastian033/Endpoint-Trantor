from pymongo import MongoClient
from datetime import datetime

conexion = MongoClient("mongodb://localhost:27017")
db_origen = conexion["DatosEmpresas"]
col_origen = db_origen.PUB_NOM_DOMICILIO

db_destino = conexion["DatosNormalizados"]
col_destino = db_destino.empresas

def normalizar_fecha(fecha):
    if not fecha or fecha in ("NaN", "nan"):
        return None
    try:
        return datetime.strptime(fecha, "%Y-%m-%d")
    except Exception:
        try:
            return datetime.fromisoformat(fecha)
        except Exception:
            return None

def norm(x):
    # Normaliza valores nulos y NaN
    return str(x or "").strip().upper().replace("NAN", "")

def key_direccion(d):
    # Usa más campos relevantes para deduplicar
    return (
        norm(d.get("tipo_direccion")),
        norm(d.get("calle")),
        norm(d.get("numero")),
        norm(d.get("comuna")),
        norm(d.get("region")),
    )

def direccion_vacia(d):
    campos = ["tipo_direccion", "calle", "numero", "comuna", "region"]
    return all(not (d.get(c) and str(d.get(c)).strip() and str(d.get(c)).upper() != "NAN") for c in campos)

def mapear_direccion(doc):
    return {
        "fecha_actualizacion": doc.get("FECHA"),
        "tipo_direccion": doc.get("TIPO_DIRECCION"),
        "calle": doc.get("CALLE"),
        "numero": doc.get("NUMERO"),
        "bloque": doc.get("BLOQUE"),
        "departamento": doc.get("DEPARTAMENTO"),
        "villa_poblacion": doc.get("VILLA_POBLACION"),
        "ciudad": doc.get("CIUDAD"),
        "comuna": doc.get("COMUNA"),
        "region": doc.get("REGION"),
        "origen": "SII",
        "vigencia_fuente": doc.get("VIGENCIA", None)
    }

procesados = 0
actualizados = 0

print("Iniciando migración de direcciones...")
total_empresas = col_destino.count_documents({})
print(f"Total de empresas a procesar: {total_empresas}")

for empresa in col_destino.find({}, {"rut": 1, "historia.direcciones": 1}):
    rut = empresa.get("rut")
    if not rut:
        continue

    print(f"\nProcesando empresa RUT: {rut}")

    # Cargar direcciones existentes
    direcciones_existentes = empresa.get("historia", {}).get("direcciones", [])
    direcciones_dict = {}

    # Indexar existentes por clave
    for d in direcciones_existentes:
        clave = key_direccion(d)
        fecha = normalizar_fecha(d.get("fecha_actualizacion"))
        direcciones_dict.setdefault(clave, []).append({
            **d,
            "fecha_dt": fecha,
            "vigencia_fuente": d.get("vigencia_fuente", None)
        })

    # Agregar/migrar desde origen
    count_nuevas = 0
    for doc in col_origen.find({"rut_completo": rut}):
        nueva = mapear_direccion(doc)
        clave = key_direccion(nueva)
        fecha = normalizar_fecha(nueva.get("fecha_actualizacion"))
        nueva["fecha_dt"] = fecha
        direcciones_dict.setdefault(clave, []).append(nueva)
        count_nuevas += 1

    print(f"  Direcciones existentes: {len(direcciones_existentes)}")
    print(f"  Direcciones nuevas encontradas en origen: {count_nuevas}")

    direcciones_final = []
    for grupo in direcciones_dict.values():
        # Ordenar por fecha y luego por vigencia fuente
        grupo.sort(key=lambda x: ((x["fecha_dt"] or datetime.min), x.get("vigencia_fuente") == "S"), reverse=True)
        for i, d in enumerate(grupo):
            d["vigente"] = (i == 0)
            d.pop("fecha_dt", None)
            d.pop("vigencia_fuente", None)
            direcciones_final.append(d)

    # Elimina direcciones completamente vacías
    direcciones_final = [d for d in direcciones_final if not direccion_vacia(d)]

    print(f"  Total direcciones finales para empresa: {len(direcciones_final)}")
    vigentes = sum(1 for d in direcciones_final if d.get("vigente"))
    print(f"  Direcciones marcadas como vigentes: {vigentes}")

    res = col_destino.update_one(
        {"_id": empresa["_id"]},
        {"$set": {"historia.direcciones": direcciones_final}}
    )
    procesados += 1
    if res.modified_count > 0:
        actualizados += 1
    if procesados % 100 == 0:
        print(f"\n{procesados} empresas procesadas hasta ahora...")

print(f"\nEmpresas procesadas: {procesados}")
print(f"Empresas actualizadas: {actualizados}")
print("Migración de direcciones finalizada.")