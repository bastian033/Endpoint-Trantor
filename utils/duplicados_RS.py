from pymongo import MongoClient
from datetime import datetime

conexion = MongoClient("mongodb://localhost:27017")
db_origen = conexion["DatosEmpresas"]
col_origen = db_origen.PUB_NOMBRES_PJ  # Cambia si es necesario

db_destino = conexion["DatosNormalizados"]
col_destino = db_destino.empresas

def normalizar_fecha(fecha):
    if not fecha or fecha in ("NaN", "nan"):
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(fecha, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(fecha)
    except Exception:
        return None

def norm(x):
    v = str(x or "").strip().upper()
    return "" if v in ("NAN", "NONE", "NULL", "") else v

def key_razon_social(d):
    return (
        norm(d.get("razon_social")),
        norm(d.get("codigo_sociedad")),
        norm(d.get("fecha_actualizacion")),
        norm(d.get("fecha_termino")),
    )

def mapear_razon_social(doc):
    return {
        "razon_social": doc.get("RAZON_SOCIAL"),
        "codigo_sociedad": doc.get("COD_SUBTIPO"),
        "fecha_actualizacion": doc.get("FECHA_TG_VIG"),
        "fecha_termino": doc.get("FECHA_FIN_VIG"),
        "origen": "SII",
        "vigente": None  # Se ajustará después
    }

procesados = 0
actualizados = 0

print("Iniciando migración de razones sociales (solo SII, preservando DatosGob históricos)...")
total_empresas = col_destino.count_documents({})
print(f"Total de empresas a procesar: {total_empresas}")

for empresa in col_destino.find({}, {"rut": 1, "historia.razon_social": 1}):
    rut = empresa.get("rut")
    if not rut:
        continue

    print(f"\nProcesando empresa RUT: {rut}")

    # 1. Trae razones sociales SII desde origen
    razones_sii = [mapear_razon_social(doc) for doc in col_origen.find({"rut_completo": rut})]
    print(f"  Razones sociales SII encontradas en origen: {len(razones_sii)}")

    # 2. Deduplicar SII
    grupos = {}
    for d in razones_sii:
        clave = key_razon_social(d)
        fecha = normalizar_fecha(d.get("fecha_actualizacion"))
        if clave in grupos:
            existente = grupos[clave]
            fecha_existente = normalizar_fecha(existente.get("fecha_actualizacion"))
            if fecha and (not fecha_existente or fecha > fecha_existente):
                grupos[clave] = d
        else:
            grupos[clave] = d

    razones_sii_final = []
    for d in grupos.values():
        if norm(d.get("fecha_termino")) == "":
            d["vigente"] = True
        else:
            d["vigente"] = False
        razones_sii_final.append(d)

    # 3. Trae razones sociales históricas de DatosGob (si existen)
    razones_destino = empresa.get("historia", {}).get("razon_social", [])
    razones_gob = [
        r for r in razones_destino
        if r.get("origen") != "SII"
        and key_razon_social(r) not in {key_razon_social(sii) for sii in razones_sii_final}
    ]

    # 4. Unir y guardar
    razones_final = razones_gob + razones_sii_final

    print(f"  Total razones sociales finales para empresa: {len(razones_final)}")
    vigentes = sum(1 for d in razones_final if d.get("vigente"))
    print(f"  Razones sociales marcadas como vigentes: {vigentes}")

    res = col_destino.update_one(
        {"_id": empresa["_id"]},
        {"$set": {"historia.razon_social": razones_final}}
    )
    procesados += 1
    if res.modified_count > 0:
        actualizados += 1
    if procesados % 100 == 0:
        print(f"\n{procesados} empresas procesadas hasta ahora...")

print(f"\nEmpresas procesadas: {procesados}")
print(f"Empresas actualizadas: {actualizados}")
print("Migración de razones sociales finalizada.")