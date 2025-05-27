from db import conexion_base_datos
from pymongo import ReplaceOne, InsertOne
from datetime import datetime

class Elimina_Duplicados:
    def __init__(self):
        self.db = conexion_base_datos().conexion()
        self.coleccion = self.db["empresas"]

    def normalizar_region(self, region):
        region_map = {
            "01": "I REGION DE TARAPACA",
            "02": "II REGION DE ANTOFAGASTA",
            "03": "III REGION DE ATACAMA",
            "04": "IV REGION DE COQUIMBO",
            "05": "V REGION DE VALPARAISO",
            "06": "VI REGION DEL LIBERTADOR GENERAL BERNARDO O'HIGGINS",
            "07": "VII REGION DEL MAULE",
            "08": "VIII REGION DEL BIOBIO",
            "09": "IX REGION DE LA ARAUCANIA",
            "10": "X REGION DE LOS LAGOS",
            "11": "XI REGION AYSEN DEL GENERAL CARLOS IBAÑEZ DEL CAMPO",
            "12": "XII REGION DE MAGALLANES Y DE LA ANTARTICA CHILENA",
            "13": "XIII REGION METROPOLITANA",
            "14": "XIV REGION DE LOS RIOS",
            "15": "XV REGION DE ARICA Y PARINACOTA",
            "16": "XVI REGION DE ÑUBLE"
        }
        if isinstance(region, int) or (isinstance(region, str) and region.isdigit()):
            return region_map.get(str(region).zfill(2), region)
        return region

    def actualizar_historial(self, campo, nuevos, existentes):
        resultado = []
        # Compara por todos los campos excepto 'vigente'
        claves = lambda x: tuple(sorted((k, v) for k, v in x.items() if k not in ["vigente"]))
        nuevos_claves = {claves(n): n for n in nuevos}
        existentes_claves = {claves(e): e for e in existentes}

        for key, nuevo in nuevos_claves.items():
            nuevo["vigente"] = True
            if key in existentes_claves:
                existente = existentes_claves[key]
                if nuevo != existente:
                    existente["vigente"] = False
                    resultado.append(existente)
                resultado.append(nuevo)
            else:
                resultado.append(nuevo)

        # Marcar los eliminados
        for key, existente in existentes_claves.items():
            if key not in nuevos_claves:
                existente["vigente"] = False
                resultado.append(existente)

        return resultado

    def procesar_lote(self, documentos_nuevos, batch_size=1000):
        operaciones = []
        for doc_nuevo in documentos_nuevos:
            try:
                rut = doc_nuevo.get("rut")
                actual = self.coleccion.find_one({"rut": rut})

                # Normalizar region en direcciones
                for direccion in doc_nuevo.get("direcciones", []):
                    direccion["region"] = self.normalizar_region(direccion.get("region"))

                # Convertir tags2 a tags (texto plano)
                if "tags2" in doc_nuevo:
                    doc_nuevo["tags"] = doc_nuevo["tags2"]
                    del doc_nuevo["tags2"]

                # Actualizar fecha de subida
                doc_nuevo["fecha_subida_datos"] = datetime.now().strftime("%Y-%m-%d")

                if not actual:
                    operaciones.append(InsertOne(doc_nuevo))
                else:
                    # Fusionar históricos
                    for campo in ["socios", "representantes_legales", "direcciones", "razon_social", "actividades_economicas", "actuacion"]:
                        nuevos = doc_nuevo.get(campo, [])
                        existentes = actual.get(campo, [])
                        doc_nuevo[campo] = self.actualizar_historial(campo, nuevos, existentes)
                    doc_nuevo["_id"] = actual["_id"]
                    operaciones.append(ReplaceOne({"_id": actual["_id"]}, doc_nuevo, upsert=True))

                # Ejecutar en lotes
                if len(operaciones) >= batch_size:
                    self.coleccion.bulk_write(operaciones, ordered=False)
                    operaciones = []
                    print(f"Procesados {batch_size} documentos en lote.")
            except Exception as e:
                print(f"Error procesando documento con rut {doc_nuevo.get('rut')}: {e}")
                
        if operaciones:
            try:
                self.coleccion.bulk_write(operaciones, ordered=False)
            except Exception as e:
                print(f"Error en bulk_write final: {e}")