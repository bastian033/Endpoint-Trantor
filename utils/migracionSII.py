from pymongo import MongoClient, InsertOne
from datetime import datetime

def norm(x):
    v = str(x or "").strip().upper()
    return "" if v in ("NAN", "NONE", "NULL", "") else v

def normalizar_rut(rut, dv):
    if not rut:
        return None
    rut = str(rut).replace(".", "").replace(" ", "").strip()
    if dv is not None:
        dv = str(dv).strip().upper()
        return f"{rut}-{dv}"
    if "-" in rut:
        partes = rut.split("-")
        if len(partes) == 2:
            return f"{partes[0]}-{partes[1].upper()}"
    return rut

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

def actividad_vacia(d):
    campos = ["actividad", "codigo_sii", "rubro", "subrubro", "region", "comuna"]
    return all(norm(d.get(c)) == "" for c in campos)

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

def key_razon_social(d):
    return (
        norm(d.get("razon_social")),
        norm(d.get("codigo_sociedad")),
        norm(d.get("fecha_actualizacion")),
        norm(d.get("fecha_termino")),
    )

def razon_social_vacia(d):
    campos = ["razon_social", "codigo_sociedad"]
    return all(norm(d.get(c)) == "" for c in campos)

conexion = MongoClient("mongodb://localhost:27018")
db_origen = conexion["DatosEmpresas"]
db_destino = conexion["DatosNormalizados"]
fecha_guardado = datetime.now().date().isoformat()

colecciones_especificas = [
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20052009",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20102014",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20152019",
    "Nómina_de_empresas_personas_jurídicas_año_comercial_20202023"
]

BATCH_SIZE = 1000

empresas_por_rut = {}

for nombre_coleccion in colecciones_especificas:
    print(f"Migrando colección: {nombre_coleccion}")
    coleccion = db_origen[nombre_coleccion]
    for doc in coleccion.find():
        rut = normalizar_rut(doc.get("RUT", ""), doc.get("DV", ""))
        if not rut:
            continue
        anio_comercial = doc.get("Año comercial")
        if anio_comercial is None:
            continue

        # Mapear actividad económica de este doc
        actividad = mapear_actividad(doc)

        if rut not in empresas_por_rut:
            empresas_por_rut[rut] = {
                "rut": rut,
                "tags": [],
                "fecha_subida_datos": fecha_guardado,
                "años_comerciales": set(),
                "historia": {
                    "socios": [],
                    "representantes_legales": [],
                    "direcciones": [
                        {
                            "vigente": True,
                            "fecha_actualizacion": doc.get("Fecha inicio de actividades vige", None),
                            "tipo_direccion": None,
                            "calle": None,
                            "numero": None,
                            "bloque": None,
                            "departamento": None,
                            "villa_poblacion": None,
                            "ciudad": None,
                            "comuna": doc.get("Comuna", None),
                            "region": doc.get("Región", None),
                            "origen": "SII"
                        }
                    ],
                    "razon_social": [
                        {
                            "razon_social": doc.get("Razón social", None),
                            "codigo_sociedad": None,
                            "fecha_actualizacion": doc.get("Fecha inicio de actividades vige", None),
                            "origen": "SII",
                            "vigente": True
                        }
                    ],
                    "actividades_economicas": [actividad],
                    "actuacion": [
                        {
                            "tipo_actuacion": None,
                            "fecha_aprobacion_SII": None,
                            "descripcion": None,
                            "origen": "SII"
                        }
                    ]
                }
            }
        else:
            actividades = empresas_por_rut[rut]["historia"]["actividades_economicas"]
            claves_existentes = {key_actividad(a) for a in actividades}
            if key_actividad(actividad) not in claves_existentes:
                actividades.append(actividad)

        empresas_por_rut[rut]["años_comerciales"].add(anio_comercial)

# Deduplicar y limpiar actividades económicas, y convertir años_comerciales a lista de dicts
for empresa in empresas_por_rut.values():
    actividades = empresa["historia"]["actividades_economicas"]
    grupos = {}
    for d in actividades:
        clave = key_actividad(d)
        grupos.setdefault(clave, []).append(d)

    actividades_final = []
    for grupo in grupos.values():
        subgrupos = {}
        for d in grupo:
            fecha_termino = norm(d.get("fecha_termino_actividades"))
            subgrupos.setdefault(fecha_termino, []).append(d)
        for sub in subgrupos.values():
            elegido = sub[0]
            for cand in sub:
                ac_elegido = int(elegido.get("Año comercial", 0)) if elegido.get("Año comercial") else 0
                ac_cand = int(cand.get("Año comercial", 0)) if cand.get("Año comercial") else 0
                if ac_cand > ac_elegido:
                    elegido = cand
            actividades_final.append(elegido)

    # elimina actividades completamente vacias
    actividades_final = [d for d in actividades_final if not actividad_vacia(d)]

    # determinar vigencia y principal
    candidatas_vigentes = [
        d for d in actividades_final if norm(d.get("fecha_termino_actividades")) == ""
    ]
    if candidatas_vigentes:
        principal = min(
            candidatas_vigentes,
            key=lambda x: normalizar_fecha(x.get("fecha_inicio_actividades")) or datetime.max
        )
        for d in actividades_final:
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
        for d in actividades_final:
            d["vigente"] = False
            d["principal"] = False

    empresa["historia"]["actividades_economicas"] = actividades_final
    empresa["años_comerciales"] = [{"año": a, "origen": "SII"} for a in sorted(empresa["años_comerciales"])]

    # --- DEDUPLICACION DE RAZONES SOCIALES ---
    razones = empresa["historia"]["razon_social"]
    grupos_rs = {}
    for d in razones:
        clave = key_razon_social(d)
        if clave not in grupos_rs:
            grupos_rs[clave] = d
        else:
            # Si hay duplicados, elige el mas reciente por fecha_actualizacion
            fecha_existente = normalizar_fecha(grupos_rs[clave].get("fecha_actualizacion"))
            fecha_nueva = normalizar_fecha(d.get("fecha_actualizacion"))
            if fecha_nueva and (not fecha_existente or fecha_nueva > fecha_existente):
                grupos_rs[clave] = d

    razones_final = [d for d in grupos_rs.values() if not razon_social_vacia(d)]

    # Determinar vigencia: vigente si no tiene fecha_termino
    for d in razones_final:
        d["vigente"] = norm(d.get("fecha_termino")) == ""

    empresa["historia"]["razon_social"] = razones_final

batch = []
for empresa in empresas_por_rut.values():
    batch.append(InsertOne(empresa))
    if len(batch) >= BATCH_SIZE:
        try:
            db_destino.empresas.bulk_write(batch, ordered=False)
        except Exception as e:
            print(f"Error en bulk_write: {e}")
        batch = []

if batch:
    try:
        db_destino.empresas.bulk_write(batch, ordered=False)
    except Exception as e:
        print(f"Error en bulk_write: {e}")

print("Migración completada.")