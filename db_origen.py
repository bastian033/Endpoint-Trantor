from pymongo import MongoClient

class conexion_base_datos_origen:
    def __init__(self):
        pass

    def conexion(self):
        cliente = MongoClient("mongodb://localhost:27017")

        db = cliente["DatosEmpresas"]
        return db