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

def key_actividad(d):
    return (
        norm(d.get("actividad")),
        norm(d.get("codigo_sii")),
        norm(d.get("fecha_inicio_actividades")),
        norm(d.get("fecha_termino_actividades")),
        norm(d.get("rubro")),
        norm(d.get("subrubro")),
    )

def mapear_actividad(doc):
    return {
        "giro": doc.get("GIRO"),
        "actividad": doc.get("ACTIVIDAD"),
        "codigo_sii": doc.get("CODIGO_SII"),
        "rubro": doc.get("RUBRO"),
        "subrubro": doc.get("SUBRUBRO"),
        "fecha_actualizacion": doc.get("FECHA_ACTUALIZACION"),
        "fecha_inicio_actividades": doc.get("FECHA_INICIO_ACTIVIDADES"),
        "fecha_termino_actividades": doc.get("FECHA_TERMINO_ACTIVIDADES"),
        "origen": "SII",
        "vigente": None,    # Se ajusta después
        "principal": None   # Se ajusta después
    }

procesados = 0
actualizados = 0

print("Iniciando migración de actividades económicas (evitando duplicados, por lotes)...")
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
        actividades_sii.extend([mapear_actividad(doc) for doc in col.find({"rut_completo": rut})])
    print(f"  Actividades SII encontradas en origen: {len(actividades_sii)}")

    # 2. Deduplicar SII
    grupos = {}
    for d in actividades_sii:
        clave = key_actividad(d)
        fecha = normalizar_fecha(d.get("fecha_inicio_actividades"))
        if clave in grupos:
            existente = grupos[clave]
            fecha_existente = normalizar_fecha(existente.get("fecha_inicio_actividades"))
            if fecha and (not fecha_existente or fecha > fecha_existente):
                grupos[clave] = d
        else:
            grupos[clave] = d

    actividades_sii_final = list(grupos.values())

    # 3. Determinar vigencia y principal
    candidatas_vigentes = [
        d for d in actividades_sii_final if norm(d.get("fecha_termino_actividades")) == ""
    ]
    if candidatas_vigentes:
        principal = max(
            candidatas_vigentes,
            key=lambda x: normalizar_fecha(x.get("fecha_inicio_actividades")) or datetime.min
        )
        for d in actividades_sii_final:
            if d is principal:
                d["vigente"] = True
                d["principal"] = True
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