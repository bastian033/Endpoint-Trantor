import os
import re
import requests
import fitz
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse, parse_qs
import nltk
import unicodedata
import string
from datetime import datetime, timedelta
from pymongo import MongoClient

# Configuración de NLTK
nltk.download('punkt')

class DiarioOficialScraper:
    def __init__(self):
        pass

    def configurar_driver(self):
        opciones = Options()
        opciones.add_argument("--headless")
        opciones.add_argument("--disable-gpu")
        opciones.add_argument("--no-sandbox")
        opciones.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opciones)

    def navegacion(self):
        #para recorrer cada dia
        fecha_base = datetime.strptime("2016-08-17", "%Y-%m-%d")
        edition_base = 41535

        ultima_fecha = self.cargar_ultima_fecha()
        fecha_inicio = datetime.strptime(ultima_fecha, "%Y-%m-%d")
        fecha_termino = datetime.today()

        fecha_actual = fecha_inicio
        while fecha_actual <= fecha_termino:
            dias_diferencia = (fecha_actual - fecha_base).days
            edition_actual = edition_base + dias_diferencia

            formato = fecha_actual.strftime("%d-%m-%Y")
            yield f"https://www.diariooficial.interior.gob.cl/edicionelectronica/empresas_cooperativas.php?date={formato}&edition={edition_actual}"

            fecha_actual += timedelta(days=1)



    def busqueda_y_guardado(self):
        driver = self.configurar_driver()
        resultados_totales = []

        try:
            for url in self.navegacion():
                print(f"Accediendo a la URL: {url}")
                driver.get(url)

                url_redirigida = driver.current_url
                print(f"URL redirigida: {url_redirigida}")

                parsed_url = urlparse(url_redirigida)
                edition = parse_qs(parsed_url.query).get("edition", [None])[0]
                fecha_actual = parse_qs(parsed_url.query).get("date", [None])[0]

                if not edition:
                    print(f"La fecha {fecha_actual} no tiene el parametro 'edition', saltando esta fecha.")
                    continue

                cliente = MongoClient("mongodb://localhost:27017/")
                db = cliente["Archivos"]
                coleccion = db["txt"]
                # hay que hacer que filtre por el nombre de cada documento no por la fecha
                if coleccion.find_one({"fecha_guardado": fecha_actual}):
                    print(f"La fecha {fecha_actual} ya fue procesada, saltando esta fecha.")
                    continue

                try:
                    categorias = WebDriverWait(driver, 30).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, "title3"))
                    )
                    pdfs = WebDriverWait(driver, 30).until(
                        EC.presence_of_all_elements_located((By.XPATH, '//a[@target="_blank"]'))
                    )
                    lista = WebDriverWait(driver, 30).until(
                        EC.presence_of_all_elements_located((By.XPATH, '//div[@style="float:left; width:550px;"]'))
                    )

                    if not categorias or not pdfs or not lista:
                        print(f"No se subio nada para la fecha {fecha_actual}.")
                        continue

                    for i, cat in enumerate(categorias):
                        nombre_categoria = cat.text.strip()
                        limite = categorias[i + 1].location["y"] if i + 1 < len(categorias) else float("inf")

                        for pdf, lis in zip(pdfs, lista):
                            if cat.location["y"] <= pdf.location["y"] < limite:
                                enlace = pdf.get_attribute("href")
                                texto_extraido = self.guardar_pdf_como_texto(enlace, nombre_categoria, lis.text)
                                resultados_totales.append({
                                    "categoria": nombre_categoria,
                                    "archivo": lis.text,
                                    "contenido": texto_extraido,
                                    "pdf_url": enlace
                                })

                    self.guardar_ultima_fecha(datetime.strptime(fecha_actual, "%d-%m-%Y").strftime("%Y-%m-%d"))

                except Exception as e:
                    print(f"Error procesando la URL {url_redirigida}: {e}")

        except Exception as e:
            print(f"Error general: {e}")
            raise

        finally:
            driver.quit()

        return resultados_totales

    def guardar_pdf_como_texto(self, enlace, categoria, nombre_archivo):
        try:
            respuesta = requests.get(enlace)
            if respuesta.status_code != 200 or not respuesta.content.startswith(b"%PDF"):
                print(f"Error al descargar: {enlace}")
                return None

            extraccion = fitz.open(stream=respuesta.content, filetype="pdf")
            texto_completo = ""
            for pagina in extraccion:
                texto_completo += pagina.get_text() + "\n"

            cliente = MongoClient("mongodb://localhost:27017/")
            db = cliente["Archivos"]
            coleccion = db["txt"]

            documento_existente = coleccion.find_one({"nombre_archivo": nombre_archivo})
            if documento_existente:
                print(f"El archivo '{nombre_archivo}' ya existe en la base de datos. Saltando insercion.")
                return texto_completo

            documento = {
                "categoria": categoria,
                "nombre_archivo": nombre_archivo,
                "pdf_url": enlace,
                "texto_completo": texto_completo,
                "fecha_guardado": datetime.now()
            }

            coleccion.insert_one(documento)
            print(f"PDF guardado en MongoDB: {nombre_archivo}")
            return texto_completo

        except Exception as e:
            print(f"Error procesando PDF: {e}")
            return None
        

    def limpieza_texto(self):
        cliente = MongoClient("mongodb://localhost:27017/")
        db = cliente["Archivos"]
        coleccion = db["txt"]

        documentos = coleccion.find()
        for documento in documentos:
            try:
                texto_limpio = self.procesar_texto(documento["texto_completo"])
                coleccion.update_one(
                    {"_id": documento["_id"]},
                    {"$set": {"texto_completo": texto_limpio}}
                )
                print(f"Texto limpio actualizado para documento con ID {documento['_id']}")
            except Exception as e:
                print(f"Error limpiando documento con ID {documento['_id']}: {e}")

    def procesar_texto(self, texto):
        sin_puntuacion = texto.translate(str.maketrans("", "", string.punctuation))
        sin_caracteres = re.sub(r"[^a-zA-Z0-9áéíóúñÁÉÍÓÚÑ\s]", "", sin_puntuacion)
        sin_espacios = re.sub(r"\s+", " ", sin_caracteres).strip()
        normalizado = unicodedata.normalize("NFD", sin_espacios)
        sin_tildes = "".join(
            char for char in normalizado if unicodedata.category(char) != "Mn"
        )
        return sin_tildes

    def limpieza_y_patrones(self):
        patrones = [
            r"\b\d{1,2}-\d{1,2}-\d{4}\b",  
            r"\bRUT\s?\d{1,2}\.\d{3}\.\d{3}-[0-9kK]\b",  
            r"\b[A-Z][a-z]+\s[A-Z][a-z]+\b", 
        ]

        cliente = MongoClient("mongodb://localhost:27017/")
        db = cliente["DatosEmpresas"]
        coleccion_txt = cliente["Archivos"]["txt"]

        documentos = coleccion_txt.find()
        for documento in documentos:
            try:
                texto_limpio = self.procesar_texto(documento["texto_completo"])
                patrones_encontrados = self.encontrar_patrones(texto_limpio, patrones)

                rut = patrones_encontrados.get(r"\bRUT\s?\d{1,2}\.\d{3}\.\d{3}-[0-9kK]\b", [None])[0]
                if not rut:
                    print(f"No se encontró RUT en el documento con ID {documento['_id']}")
                    continue

                rut_encontrado = False
                for nombre_coleccion in db.list_collection_names():
                    coleccion = db[nombre_coleccion]
                    documento_existente = coleccion.find_one({"rut": rut})

                    if documento_existente:
                        rut_encontrado = True
                        cambios = {}
                        for clave, valor in patrones_encontrados.items():
                            if clave not in documento_existente or documento_existente[clave] != valor:
                                cambios[clave] = valor

                        if cambios:
                            coleccion.update_one(
                                {"rut": rut},
                                {"$set": cambios, "$push": {"historial": {"documento_id": documento["_id"], "fecha": datetime.now()}}},
                            )
                            print(f"Datos actualizados para RUT {rut} en la colección {nombre_coleccion}")
                        break 

                if not rut_encontrado:
                    coleccion_predeterminada = db["datos_extraidos"]
                    nuevo_documento = {
                        "rut": rut,
                        "datos": patrones_encontrados,
                        "historial": [{"documento_id": documento["_id"], "fecha": datetime.now()}],
                    }
                    coleccion_predeterminada.insert_one(nuevo_documento)
                    print(f"Nuevo documento insertado para RUT {rut} en la colección predeterminada")

            except Exception as e:
                print(f"Error procesando documento con ID {documento['_id']}: {e}")


    def encontrar_patrones(self, texto, patrones):
        resultados = {}
        for patron in patrones:
            coincidencias = re.findall(patron, texto)
            if coincidencias:
                resultados[patron] = coincidencias
        return resultados


    def guardar_en_mongodb(self, datos):
        cliente = MongoClient("mongodb://localhost:27017/")
        db = cliente["DatosEmpresas"]
        coleccion = db["datos_extraidos"]

        for dato in datos:
            coleccion.update_one(
                {"rut": dato["rut"]},
                {"$push": {"historial": dato}},
                upsert=True
            )
        print("Datos guardados en MongoDB.")

    def guardar_ultima_fecha(self, fecha):
        with open("ultima_fecha.txt", "w") as archivo:
            archivo.write(fecha)

    def cargar_ultima_fecha(self):
        if os.path.exists("ultima_fecha.txt"):
            with open("ultima_fecha.txt", "r") as archivo:
                return archivo.read().strip()
        return "2016-08-17"


    def eliminar_duplicados(self):
        cliente = MongoClient("mongodb://localhost:27017/")
        db = cliente["Archivos"]
        coleccion = db["txt"]
        pipeline = [
            {"$group": {
                "_id": {"nombre_archivo": "$nombre_archivo"}, 
                "ids": {"$push": "$_id"},
                "conteo": {"$sum": 1} 
            }},
            {"$match": {"conteo": {"$gt": 1}}} 
        ]

        duplicados = list(coleccion.aggregate(pipeline))

        for doc in duplicados:
            ids = doc["ids"]
            ids_a_eliminar = ids[1:] 
            coleccion.delete_many({"_id": {"$in": ids_a_eliminar}})
            print(f"eliminados {len(ids_a_eliminar)} duplicados para nombre del archivo: {doc['_id']['nombre_archivo']}")

        print("Duplicados eliminados.")

        try:
            coleccion.create_index("nombre_archivo", unique=True)
            print("Índice único creado en el campo 'nombre_archivo'.")
        except Exception as e:
            print(f"Error al crear el índice único: {e}")




if __name__ == "__main__":
    scraper = DiarioOficialScraper()
    scraper.busqueda_y_guardado()
    scraper.limpieza_texto()
    scraper.limpieza_y_patrones()




