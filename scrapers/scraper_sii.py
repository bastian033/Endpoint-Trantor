import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scrapers.scraper_diario_oficial import DiarioOficialScraper
from pymongo import MongoClient
import shutil
import requests
import zipfile
import re
from datetime import datetime, date

from utils.txt_to_json import transformar_txt_a_json

class SII_Scraper:

    def __init__(self):
        self.ruta_descarga = os.path.join(os.path.expanduser("~"), "Documents")

    def busqueda_y_descarga(self):
        scraper = DiarioOficialScraper()
        driver = scraper.configurar_driver()
        driver.get("https://www.sii.cl/sobre_el_sii/nominapersonasjuridicas.html")
        fuente_general = "https://www.sii.cl/sobre_el_sii/nominapersonasjuridicas.html"

        colecciones_actualizadas = []

        try:
            div_contenido = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "col-sm-9.contenido"))
            )

            for li in div_contenido.find_elements(By.TAG_NAME, "li"):
                for url in li.find_elements(By.TAG_NAME, "a"):
                    enlaces = url.get_attribute("href")
                    respuesta = requests.get(enlaces)

                    # para extraer la fecha de actualizacion
                    fecha_actualizacion = None
                    for fecha in li.find_elements(By.CLASS_NAME, "fecha-actualizacion"):
                        fecha_texto = fecha.text.strip()
                        patron = r"\(Actualización:\s+([a-zA-Z]+)\s+(\d{4})\)"
                        resultado = re.search(patron, fecha_texto)
                        if resultado:
                            mes_str, anio_str = resultado.groups()
                            meses = {
                                "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
                                "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
                            }
                            mes = meses.get(mes_str.lower())
                            anio = int(anio_str)
                            if mes:
                                fecha_actualizacion = date(anio, mes, 1)
                            break

                    if respuesta.status_code == 200:
                        nombre_zip = url.text.strip().replace(" ", "_") + ".zip"
                        nomina = os.path.join(self.ruta_descarga, nombre_zip)
                        with open(nomina, "wb") as archivo:
                            archivo.write(respuesta.content)
                        print(f"descargado: {nomina}")

                        ruta_extraida = os.path.join(self.ruta_descarga, "extraidos")
                        self.limpiar_carpeta(ruta_extraida)
                        self.extraer_zip(nomina, ruta_extraida)

                        for archivo in os.listdir(ruta_extraida):
                            if archivo.endswith(".txt"):
                                ruta_txt = os.path.join(ruta_extraida, archivo)
                                nombre_archivo = os.path.basename(ruta_txt)
                                nombre_sin_extension = os.path.splitext(nombre_archivo)[0]

                                # para consultar si ya se proceso este archivo y fecha
                                procesado = False
                                if fecha_actualizacion:
                                    cliente = MongoClient("mongodb://localhost:27017/")
                                    db = cliente["DatosEmpresas"]
                                    registro = db["actualizaciones"].find_one({
                                        "coleccion": nombre_sin_extension,
                                        "fecha": str(fecha_actualizacion)
                                    })
                                    cliente.close()
                                    if registro:
                                        print(f"ya existe {nombre_sin_extension} con fecha {fecha_actualizacion}, se saltara la descarga")
                                        procesado = True

                                if procesado:
                                    continue

                                self.subir_txt_a_mongodb(ruta_txt, fuente_general)
                                colecciones_actualizadas.append(nombre_sin_extension)  # <-- Agrega a la lista

                                if fecha_actualizacion:
                                    cliente = MongoClient("mongodb://localhost:27017/")
                                    db = cliente["DatosEmpresas"]
                                    db["actualizaciones"].insert_one({
                                        "coleccion": nombre_sin_extension,
                                        "fecha": str(fecha_actualizacion),
                                        "descargado": True,
                                        "timestamp": datetime.now()
                                    })
                                    cliente.close()

        except Exception as e:
            print(f"Error: {e}")

        finally:
            driver.quit()

        return colecciones_actualizadas

    def extraer_zip(self, nomina, ruta_destino):
        try:
            with zipfile.ZipFile(nomina, 'r') as zip_extraido:
                zip_extraido.extractall(ruta_destino)
                print(f"extraido en: {ruta_destino}")
        except Exception as e:
            print(f"error al extraer: {e}")

    def limpiar_nombre(self, nombre):
        nombre = re.sub(r"[^\w\s]", "", nombre)
        nombre = nombre.replace(" ", "_")
        return nombre

    def subir_txt_a_mongodb(self, ruta_txt, fuente):
        cliente = MongoClient("mongodb://localhost:27017/")
        try:
            db = cliente["DatosEmpresas"]
            nombre_archivo = os.path.basename(ruta_txt)
            nombre_sin_extension = os.path.splitext(nombre_archivo)[0]
            coleccion = db[nombre_sin_extension]
            transformador = transformar_txt_a_json([ruta_txt])
            for lote in transformador.convertir_txt_a_json():
                for doc in lote:
                    doc["fuente"] = fuente
                self.insertar_en_lotes(coleccion, lote)
        except Exception as e:
            print(f"Error al subir a MongoDB: {e}")
        finally:
            cliente.close()
            print(f"subida completa: {nombre_sin_extension}")

    def insertar_en_lotes(self, coleccion, documentos, tamaño_lote=30):
        for i in range(0, len(documentos), tamaño_lote):
            lote = documentos[i:i + tamaño_lote]
            try:
                coleccion.insert_many(lote)
                print(f"Subido lote {i} - {i + len(lote) - 1}")
            except Exception as e:
                print(f"Error subiendo lote {i}: {e}")

    def limpiar_carpeta(self, ruta):
        if os.path.exists(ruta):
            for archivo in os.listdir(ruta):
                ruta_archivo = os.path.join(ruta, archivo)
                if os.path.isfile(ruta_archivo):
                    os.remove(ruta_archivo)
                elif os.path.isdir(ruta_archivo):
                    #esto es para eliminar sub carpetas
                    shutil.rmtree(ruta_archivo)

    def obtener_nombres_colecciones(self):
        cliente = MongoClient("mongodb://localhost:27017/")
        try:
            db = cliente["DatosEmpresas"]
            return db.list_collection_names()
        except Exception as e:
            print(f"Error al obtener colecciones: {e}")
            return []
        finally:
            cliente.close()

    def registrar_revision(self, fuente):
        cliente = MongoClient("mongodb://localhost:27017/")
        db = cliente["DatosEmpresas"]
        db["revisiones"].update_one(
            {"fuente": fuente},
            {"$set": {"fecha_revision": datetime.now()}},
            upsert=True
        )
        cliente.close()

if __name__ == "__main__":
    scraper = SII_Scraper()
    colecciones_actualizadas = scraper.busqueda_y_descarga()
    if colecciones_actualizadas:
        print("Colecciones actualizadas:", colecciones_actualizadas)
    scraper.registrar_revision("SII")