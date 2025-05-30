from pymongo import MongoClient
from datetime import datetime

conexion = MongoClient("mongodb://localhost:27017")
db_origen = conexion["DatosEmpresas"]
col_origen = db_origen.PUB_NOM_SUCURSAL 

db_destino = conexion["DatosNormalizados"]
col_destino = db_destino.empresas

def normalizar_fecha(fecha):
    if not fecha or fecha in ("NaN", "nan"):
        return None
    # Soporta formatos "YYYY-MM-DD" y "DD-MM-YYYY"
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

def key_direccion(d):
    # Clave robusta para deduplicar direcciones
    return (
        norm(d.get("tipo_direccion")),
        norm(d.get("calle")),
        norm(d.get("numero")),
        norm(d.get("departamento")),
        norm(d.get("comuna")),
        norm(d.get("region")),
    )

def direccion_vacia(d):
    campos = ["tipo_direccion", "calle", "numero", "comuna", "region"]
    return all(not (d.get(c) and str(d.get(c)).strip() and str(d.get(c)).upper() not in ("NAN", "NONE", "NULL", "")) for c in campos)

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
        "vigencia_sii": doc.get("VIGENCIA", None)
    }

procesados = 0
actualizados = 0

print("Iniciando migración de direcciones (evitando duplicados)...")
total_empresas = col_destino.count_documents({})
print(f"Total de empresas a procesar: {total_empresas}")

for empresa in col_destino.find({}, {"rut": 1, "historia.direcciones": 1}):
    rut = empresa.get("rut")
    if not rut:
        continue

    print(f"\nProcesando empresa RUT: {rut}")

    # Migrar todas las direcciones del origen para este rut
    direcciones = [mapear_direccion(doc) for doc in col_origen.find({"rut_completo": rut})]
    print(f"  Direcciones encontradas en origen: {len(direcciones)}")

    # Deduplicar: solo una dirección por clave, la más reciente
    grupos = {}
    for d in direcciones:
        clave = key_direccion(d)
        fecha = normalizar_fecha(d.get("fecha_actualizacion"))
        if clave in grupos:
            existente = grupos[clave]
            fecha_existente = normalizar_fecha(existente.get("fecha_actualizacion"))
            if fecha and (not fecha_existente or fecha > fecha_existente):
                grupos[clave] = d
        else:
            grupos[clave] = d

    # Asignar 'vigente' booleano según la lógica de SII
    direcciones_final = []
    for d in grupos.values():
        d["vigente"] = True if d.get("vigencia_sii") == "S" else False
        d.pop("vigencia_sii", None)
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