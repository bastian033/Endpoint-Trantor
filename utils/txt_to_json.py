import pandas as pd
import json

class transformar_txt_a_json:

    def __init__(self, rutas_archivos, tamaño_chunk=10000):
        self.rutas_archivos = rutas_archivos
        self.tamaño_chunk = tamaño_chunk


    def convertir_txt_a_json(self):

        for ruta in self.rutas_archivos:
            print(f"procesando: {ruta}")
            for chunk in pd.read_csv(ruta, sep="\t", chunksize=self.tamaño_chunk, engine="python"):
                data = chunk.to_dict(orient="records")
                yield data
    






