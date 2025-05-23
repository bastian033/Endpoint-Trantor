from pymongo import MongoClient, UpdateOne

conexion = MongoClient("mongodb://localhost:27018")
db = conexion["DatosNormalizados"]
coleccion = db.empresas

BATCH_SIZE = 1000
batch = []

cursor = coleccion.find({}, {"historia": 1, "nombre_de_fantasia": 1, "nombre_fantasia": 1})

for doc in cursor:
    tags = set()

    # Razon social
    for rs in doc.get("historia", {}).get("razon_social", []):
        if rs.get("razon_social"):
            tags.add(str(rs["razon_social"]))

    # Socios
    for socio in doc.get("historia", {}).get("socios", []):
        if socio.get("nombre"):
            tags.add(str(socio["nombre"]))
        if socio.get("rut"):
            tags.add(str(socio["rut"]))

    # Representantes legales
    for rep in doc.get("historia", {}).get("representantes_legales", []):
        if rep.get("nombre"):
            tags.add(str(rep["nombre"]))
        if rep.get("rut"):
            tags.add(str(rep["rut"]))

    # Nombre de fantasia
    nombre_fantasia = doc.get("nombre_de_fantasia") or doc.get("nombre_fantasia")
    if nombre_fantasia:
        tags.add(str(nombre_fantasia))

    if tags:
        batch.append(
            UpdateOne({"_id": doc["_id"]}, {"$set": {"tags": list(tags)}})
        )

    if len(batch) >= BATCH_SIZE:
        try:
            coleccion.bulk_write(batch, ordered=False)
        except Exception as e:
            print(f"Error en bulk_write: {e}")
        batch = []

if batch:
    try:
        coleccion.bulk_write(batch, ordered=False)
    except Exception as e:
        print(f"Error en bulk_write final: {e}")

print("Campo tags actualizado como lista.")

# Crear índices recomendados
coleccion.create_index("rut")
coleccion.create_index("tags")
print("Índices creados.")