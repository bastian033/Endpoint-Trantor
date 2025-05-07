import unicodedata
import re

from ..db import db

class campo_normalizado:

    def __init__(self):
        self.db = db


    def normalizar_rut(self, rut, dv):
        rut = str(rut).strip().replace(".", "").replace("-", "")
        if dv is not None:
            dv = str(dv).strip().upper()
            return f"{rut}-{dv}"

        if len(rut) > 1 and rut[-2] == "-":
            return rut.upper()
        
        return rut
        
    

    def normalizar_razon_social(self, razon_social):
        if not isinstance(razon, str):
            return None
        
        #Elimina tildes y comas
        #"NFKC" significa Normalization Form Compatibility Composition
        razon = unicodedata.normalize("NFKC", razon).encode("ascii", "ignore").decode('utf-8')

        razon = razon.lower()

        #Elimina caracteres especiales
        razon = re.sub(r'[^a-z0-9\s]','', razon)

        #Elimina espacios dobles
        razon = re.sub(r'\s+', ' ', razon)

        razon = razon.strip()

        return razon



    def procesar_coleccion(self, coleccion_nombre):
        coleccion = self.db[coleccion_nombre]
        for campo in coleccion.find():
            rut = campo.get("RUT") or campo.get("Rut")
            dv = campo.get("DV") or campo.get("Dv") or campo.get("dv")
            razon_social = campo.get("Razón social") or campo.get("Razon Social") or campo.get("Razon_Social") or campo.get("Razón_Social") or campo.get("RAZON_SOCIAL") or campo.get("RAZÓN_SOCIAL") or campo.get("RAZÓN SOCIAL") or campo.get("RAZON SOCIAL") or campo.get("razon social")

            rut_normalizado = self.normalizar_rut(rut, dv)
            razon_normalizada = self.normalizar_razon_social(razon_social)

            coleccion.update_one(
                {"_id": campo["_id"]},
                {"$set": {
                    "rut_normalizado": rut_normalizado,
                    "razon_social_normalizada": razon_normalizada
                }}
            )


    def normalizar_colecciones(self):
        for coleccion_nombre in self.db.list_collection_names():
            print(f"procesando coleccion:{coleccion_nombre}")
            self.procesar_coleccion(coleccion_nombre)

                
if __name__ == "__main__":
    normalizado = campo_normalizado()
    normalizado.normalizar_colecciones()