import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_destino import conexion_base_datos

class AddFechaSubidaDatos:
    def __init__(self):
        self.db = conexion_base_datos().conexion()

    def run(self):
        for doc in self.db.empresas.find():
            fecha_subida = doc.get("fecha_subida_datos", None)
            if not fecha_subida:
                continue

            updates = {}

            # Actualiza direcciones
            direcciones = doc.get("historia", {}).get("direcciones", [])
            nuevas_direcciones = []
            for d in direcciones:
                if "fecha_subida_datos" not in d:
                    d["fecha_subida_datos"] = fecha_subida
                nuevas_direcciones.append(d)
            updates["historia.direcciones"] = nuevas_direcciones

            # Actualiza razon_social
            razon_social = doc.get("historia", {}).get("razon_social", [])
            nuevas_razon_social = []
            for r in razon_social:
                if "fecha_subida_datos" not in r:
                    r["fecha_subida_datos"] = fecha_subida
                nuevas_razon_social.append(r)
            updates["historia.razon_social"] = nuevas_razon_social

            #para actualizar actividades_economicas
            actividades_economicas = doc.get("historia", {}.get("actividades_economicas", []))
            nuevas_actuacion = []
            for a in actividades_economicas:
                if "fecha_subida_datos" not in a:
                    a["fecha_subida_datos"] = fecha_subida
                    nuevas_actuacion.append(a)
                updates["historia.actividades_economicas"] = nuevas_actuacion

            # Actualiza actuacion
            actuacion = doc.get("historia", {}).get("actuacion", [])
            nuevas_actuacion = []
            for a in actuacion:
                if "fecha_subida_datos" not in a:
                    a["fecha_subida_datos"] = fecha_subida
                nuevas_actuacion.append(a)
            updates["historia.actuacion"] = nuevas_actuacion

            self.db.empresas.update_one({"_id": doc["_id"]}, {"$set": updates})

        print("Actualización de fecha_subida_datos completada.")

if __name__ == "__main__":
    AddFechaSubidaDatos().run()