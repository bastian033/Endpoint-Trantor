from pymongo import MongoClient


class conexion_base_datos:
    def __init__(self):
        pass

    def conexion(self):
        cliente = MongoClient("mongodb://localhost:27018")

        db = cliente["DatosNormalizados"]
        return db