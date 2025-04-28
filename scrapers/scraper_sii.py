import selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scraper import DiarioOficialScraper
from pymongo import MongoClient
import shutil 
import requests
import zipfile
import os # sistema operativo
import sys
import re # para expresiones regulares

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) # para que pueda importar desde utils
from utils.txt_to_json import transformar_txt_a_json

class SII_Scraper:

    def __init__(self):
        self.ruta_descarga = os.path.join(os.path.expanduser("~"), "Documents") 

    def busqueda_y_descarga(self):
        scraper = DiarioOficialScraper()
        driver = scraper.configurar_driver()
        driver.get("https://www.sii.cl/sobre_el_sii/nominapersonasjuridicas.html")
        fuente_general = "https://www.sii.cl/sobre_el_sii/nominapersonasjuridicas.html"

        try:
            div_contenido = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "col-sm-9.contenido"))
            )

            for li in div_contenido.find_elements(By.TAG_NAME, "li"):
                for url in li.find_elements(By.TAG_NAME, "a"):
                    enlaces = url.get_attribute("href")
                    nombre_txt = url.text.replace(" ", "_")
                    print(nombre_txt)
                    if nombre_txt in self.obtener_nombres_colecciones():
                        print(f"Ya existe la coleccion: {nombre_txt}")
                        continue

                    respuesta = requests.get(enlaces)
                    if respuesta.status_code == 200:
                        nombre_zip = os.path.join(self.ruta_descarga, url.text.strip())
                        with open(nombre_zip, "wb") as archivo:  
                            archivo.write(respuesta.content)
                        print(f"descargado: {nombre_zip}")
                        
                        ruta_extraida = os.path.join(self.ruta_descarga, "extraidos")
                        self.limpiar_carpeta(ruta_extraida)
                        self.extraer_zip(nombre_zip, ruta_extraida)
                        
                        for archivo in os.listdir(ruta_extraida):
                            if archivo.endswith(".txt"): 
                                ruta_txt = os.path.join(ruta_extraida, archivo)
                                self.subir_txt_a_mongodb(ruta_txt, nombre_txt, fuente_general)

        except Exception as e:
            print(f"Error: {e}")

        finally:
            driver.quit()

    def extraer_zip(self, nombre_zip, ruta_destino):
        try:
            with zipfile.ZipFile(nombre_zip, 'r') as zip_extraido:
                zip_extraido.extractall(ruta_destino)
                print(f"extraido en: {ruta_destino}")
        except Exception as e:
            print(f"error al extraer: {e}")

    def limpiar_nombre(self, nombre):
        nombre = re.sub(r"[^\w\s]", "", nombre)
        nombre = nombre.replace(" ", "_")
        return nombre


    def subir_txt_a_mongodb(self, ruta_txt, nombre_txt, fuente):
            cliente = MongoClient("mongodb://localhost:27017/")
            try:
                db = cliente["DatosEmpresas"]
                nombre_coleccion = self.limpiar_nombre(nombre_txt)
                coleccion = db[nombre_coleccion]

                transformador = transformar_txt_a_json([ruta_txt])
                jsons = transformador.convertir_txt_a_json()
                
                for doc in jsons:
                    doc["fuente"] = fuente

                self.insertar_en_lotes(coleccion, jsons)

            except Exception as e:
                print(f"Error al subir a MongoDB: {e}")

            finally:
                cliente.close()

    def insertar_en_lotes(self,coleccion, documentos, tamaño_lote=50):
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


if __name__ == "__main__":
    scraper = SII_Scraper()
    scraper.busqueda_y_descarga()