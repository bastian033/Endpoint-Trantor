import pandas as pd
import json

class transformar_txt_a_json:

    def __init__(self, rutas_archivos):
        self.rutas_archivos = rutas_archivos


    def convertir_txt_a_json(self):

        jsons = []

        for ruta in self.rutas_archivos:

            df = pd.read_csv(ruta, sep="\t", engine="python")

            jsons_str = df.to_json(orient="records")

            data = json.loads(jsons_str)

            jsons.extend(data)

        return jsons
    






