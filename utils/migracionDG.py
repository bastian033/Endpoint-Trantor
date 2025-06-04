from datetime import datetime
from db_origen import conexion_base_datos_origen
from db_destino import conexion_base_datos as conexion_base_datos_destino

class MigracionDG:
    def __init__(self):
        self.db_origen = conexion_base_datos_origen().conexion()
        self.db_destino = conexion_base_datos_destino().conexion()
        self.fecha_guardado = datetime.now().date().isoformat()
        self.colecciones = [f"DatosGob{anio}" for anio in range(2013, 2025)]

    def migrar(self):
        print(f"Total de colecciones: {self.colecciones}")
        for nombre_coleccion in self.colecciones:
            print(f"Migrando colección: {nombre_coleccion}")
            coleccion = self.db_origen[nombre_coleccion]
            cursor = coleccion.find()
            for doc in cursor:
                rut = doc.get("RUT", None)
                direccion = {
                    "vigente": True,
                    "fecha_actualizacion": doc.get("Fecha de aprobacion x SII", None),
                    "fecha_subida_datos": self.fecha_guardado,
                    "tipo_direccion": None,
                    "calle": None,
                    "numero": None,
                    "bloque": None,
                    "departamento": None,
                    "villa_poblacion": None,
                    "ciudad": None,
                    "comuna": doc.get("Comuna Tributaria", None),
                    "region": doc.get("Region Tributaria", None),
                    "origen": "DatosGob"
                }
                razon_social = {
                    "razon_social": doc.get("Razon Social", None),
                    "codigo_sociedad": doc.get("Codigo de sociedad", None),
                    "fecha_actualizacion": doc.get("Fecha de aprobacion x SII", None),
                    "fecha_subida_datos": self.fecha_guardado,
                    "origen": "DatosGob",
                    "vigente": True
                }
                actuacion = {
                    "tipo_actuacion": doc.get("Tipo de actuacion", None),
                    "fecha_aprobacion_SII": doc.get("Fecha de aprobacion x SII", None),
                    "fecha_subida_datos": self.fecha_guardado,
                    "origen": "DatosGob"
                }
                self.db_destino.empresas.update_one(
                    {"rut": rut},
                    {
                        "$setOnInsert": {
                            "rut": rut,
                            "tags": [],
                            "fecha_subida_datos": self.fecha_guardado,
                            "historia": {
                                "socios": [],
                                "representantes_legales": [],
                                "direcciones": [],
                                "razon_social": [],
                                "actividades_economicas": [],
                                "actuacion": []
                            }
                        },
                        "$push": {
                            "historia.direcciones": direccion,
                            "historia.razon_social": razon_social,
                            "historia.actuacion": actuacion
                        }
                    },
                    upsert=True
                )
        print("Migración completada.")

if __name__ == "__main__":
    MigracionDG().migrar()