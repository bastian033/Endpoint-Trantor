from pymongo import MongoClient
from datetime import datetime

conexion = MongoClient("mongodb://localhost:27017")
db_origen = conexion["DatosEmpresas"]

colecciones_origen = [
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20052009",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20102014",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20152019",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20202023"
]

db_destino = conexion["DatosNormalizados"]
col_destino = db_destino.empresas

BATCH_SIZE = 200

def norm(x):
    v = str(x or "").strip().upper()
    return "" if v in ("NAN", "NONE", "NULL", "") else v

def normalizar_fecha(fecha):
    if not fecha or str(fecha).upper() in ("NAN", "NONE", "NULL", ""):
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

def key_actividad(d):
    return (
        norm(d.get("actividad")),
        norm(d.get("codigo_sii")),
        norm(d.get("rubro")),
        norm(d.get("subrubro")),
        norm(d.get("region")),
        norm(d.get("comuna")),
    )

def mapear_actividad(doc):
    return {
        "giro": doc.get("GIRO"),
        "actividad": doc.get("Actividad económica") or doc.get("ACTIVIDAD"),
        "codigo_sii": doc.get("CODIGO_SII"),
        "rubro": doc.get("Rubro económico") or doc.get("RUBRO"),
        "subrubro": doc.get("Subrubro económico") or doc.get("SUBRUBRO"),
        "region": doc.get("Región") or doc.get("REGION"),
        "comuna": doc.get("Comuna") or doc.get("COMUNA"),
        "fecha_actualizacion": doc.get("FECHA_ACTUALIZACION"),
        "fecha_inicio_actividades": doc.get("Fecha inicio de actividades vige") or doc.get("FECHA_INICIO_ACTIVIDADES"),
        "fecha_termino_actividades": doc.get("Fecha término de giro") or doc.get("FECHA_TERMINO_ACTIVIDADES"),
        "origen": "SII",
        "vigente": None,  
        "principal": None   
    }

def actividad_vacia(d):
    campos = ["actividad", "codigo_sii", "rubro", "subrubro", "region", "comuna"]
    return all(norm(d.get(c)) == "" for c in campos)

procesados = 0
actualizados = 0

print("Iniciando migracion de actividades economicas")
total_empresas = col_destino.count_documents({})
print(f"Total de empresas a procesar: {total_empresas}")

cursor = col_destino.find({}, {"rut": 1, "historia.actividades_economicas": 1})
batch = []
for empresa in cursor:
    rut = empresa.get("rut")
    if not rut:
        continue

    print(f"\nProcesando empresa RUT: {rut}")

    # 1. Trae actividades económicas SII desde las 4 colecciones de origen
    actividades_sii = []
    for nombre_col in colecciones_origen:
        col = db_origen[nombre_col]
        actividades_sii.extend([mapear_actividad(doc) for doc in col.find({"rut_unificado": rut})])
    print(f"  Actividades SII encontradas en origen: {len(actividades_sii)}")

    # 2. Agrupa por clave robusta
    grupos = {}
    for d in actividades_sii:
        clave = key_actividad(d)
        grupos.setdefault(clave, []).append(d)

    actividades_sii_final = []
    for grupo in grupos.values():
        # Agrupa por fecha de termino
        subgrupos = {}
        for d in grupo:
            fecha_termino = norm(d.get("fecha_termino_actividades"))
            subgrupos.setdefault(fecha_termino, []).append(d)
        for sub in subgrupos.values():
            elegido = sub[0]
            for cand in sub:
                # Si hay campo "Año comercial", elige el más alto
                ac_elegido = int(elegido.get("Año comercial", 0)) if elegido.get("Año comercial") else 0
                ac_cand = int(cand.get("Año comercial", 0)) if cand.get("Año comercial") else 0
                if ac_cand > ac_elegido:
                    elegido = cand
            actividades_sii_final.append(elegido)

    # Elimina actividades completamente vacías
    actividades_sii_final = [d for d in actividades_sii_final if not actividad_vacia(d)]

    # 3. Determinar vigencia y principal
    candidatas_vigentes = [
        d for d in actividades_sii_final if norm(d.get("fecha_termino_actividades")) == ""
    ]
    if candidatas_vigentes:
        # Principal: la de fecha de inicio más antigua (o más reciente si prefieres)
        principal = min(
            candidatas_vigentes,
            key=lambda x: normalizar_fecha(x.get("fecha_inicio_actividades")) or datetime.max
        )
        for d in actividades_sii_final:
            if d is principal:
                d["vigente"] = True
                d["principal"] = True
            elif d in candidatas_vigentes:
                d["vigente"] = True
                d["principal"] = False
            else:
                d["vigente"] = False
                d["principal"] = False
    else:
        for d in actividades_sii_final:
            d["vigente"] = False
            d["principal"] = False

    # 4. Preserva actividades históricas de otros orígenes (DatosGob, etc.)
    actividades_destino = empresa.get("historia", {}).get("actividades_economicas", [])
    actividades_gob = [
        a for a in actividades_destino
        if a.get("origen") != "SII"
        and key_actividad(a) not in {key_actividad(sii) for sii in actividades_sii_final}
    ]

    # 5. Unir y guardar
    actividades_final = actividades_gob + actividades_sii_final

    print(f"  Total actividades económicas finales para empresa: {len(actividades_final)}")
    vigentes = sum(1 for d in actividades_final if d.get("vigente"))
    print(f"  Actividades marcadas como vigentes: {vigentes}")
    principales = sum(1 for d in actividades_final if d.get("principal"))
    print(f"  Actividades marcadas como principal: {principales}")

    batch.append({
        "filter": {"_id": empresa["_id"]},
        "update": {"$set": {"historia.actividades_economicas": actividades_final}}
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
print("Migración de actividades económicas finalizada.")