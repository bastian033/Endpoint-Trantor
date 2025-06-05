from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import requests
import csv
import sys
import os
import re
import datetime
from pymongo import MongoClient

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db_origen import conexion_base_datos_origen

class ScraperDatosGob:
    def __init__(self):
        self.conexion_db = conexion_base_datos_origen().conexion()

    def configuracion(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox") # para evitar errores
        options.add_argument("--disable-dev-shm-usage") # para usar el disco para los datos temporales y no la memoria compartida 
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)

    def obtener_fecha_actualizacion(self, driver):
        try:
            tabla = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-striped"))
            )
            filas = tabla.find_elements(By.TAG_NAME, "tr")
            for fila in filas:
                ths = fila.find_elements(By.TAG_NAME, "th") # texto fecha datos
                tds = fila.find_elements(By.TAG_NAME, "td") # fecha 
                if ths and tds and "Última actualización de los datos" in ths[0].text:
                    fecha_texto = tds[0].text.strip()
                    patron = r"(\d{1,2}) de ([a-zA-Z]+) de (\d{4})"
                    resultado = re.search(patron, fecha_texto)
                    if resultado:
                        dia, mes_str, anio = resultado.groups()
                        meses = {
                            "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
                            "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
                        }
                        mes = meses.get(mes_str.lower())
                        if mes:
                            return datetime.date(int(anio), mes, int(dia))
                    return fecha_texto
            return None
        except Exception as e:
            print(f"Error obteniendo fecha de actualizacion: {e}")
            return None

    def es_actualizacion_nueva(self, anio, fecha_actual):
        cliente = self.conexion_db.client
        db = cliente["DatosGob"]
        registro = db["actualizaciones"].find_one({"fuente": "datosgob", "anio": anio})
        if registro and "fecha" in registro:
            return str(fecha_actual) > registro["fecha"]
        return True

    def guardar_fecha_actualizacion(self, anio, fecha_actual):
        cliente = self.conexion_db.client
        db = cliente["DatosGob"]
        db["actualizaciones"].update_one(
            {"fuente": "datosgob", "anio": anio},
            {"$set": {"fecha": str(fecha_actual), "timestamp": datetime.datetime.now()}},
            upsert=True
        )

    def subir_a_mongodb(self, ruta_archivo, tamaño_lote=1000):
        try:
            nombre_archivo = os.path.basename(ruta_archivo)
            match = re.search(r"(20\d{2})", nombre_archivo)
            anio = match.group(1) if match else "desconocido"

            coleccion = self.conexion_db[f"DatosGob{anio}"]

            with open(ruta_archivo, newline='', encoding='utf-8') as csvfile:
                lector = csv.DictReader(csvfile)
                lote = []
                total = 0
                for doc in lector:
                    lote.append(doc)
                    if len(lote) >= tamaño_lote:
                        coleccion.insert_many(lote)
                        total += len(lote)
                        print(f"{total} documentos insertados en la coleccion '{anio}'")
                        lote = []
                if lote:
                    coleccion.insert_many(lote)
                    total += len(lote)
                    print(f"{total} documentos insertados en la coleccion '{anio}'")
            if total == 0:
                print("el archivo csv esta vacio o malo")

        except Exception as e:
            print(f"error al subir a mongodb: {e}")

    def descargar_csv(self, url):
        try:
            match = re.search(r"(20\d{2})", url)
            anio = match.group(1) if match else "desconocido"

            os.makedirs("csvs", exist_ok=True)
            nombre_archivo = f"DatosGob_{anio}.csv"
            ruta_completa = os.path.join("csvs", nombre_archivo)

            respuesta = requests.get(url)
            respuesta.raise_for_status() 

            with open(ruta_completa, "wb") as f:
                f.write(respuesta.content)

            print(f"CSV descargado correctamente: {ruta_completa}")
            self.subir_a_mongodb(ruta_completa)
            return ruta_completa

        except Exception as e:
            print(f"Error al descargar CSV desde {url}: {e}")
            return None

    def navegacion(self, url, anio):
        print(f"navegando a {url}")
        driver = self.configuracion()
        try:
            driver.get(url)
            fecha_actual = self.obtener_fecha_actualizacion(driver)
            if fecha_actual and self.es_actualizacion_nueva(anio, fecha_actual):
                enlace_csv = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "resource-url-analytics"))
                ).get_attribute("href")
                print(f"csv encontrado: {enlace_csv}")
                ruta_csv = self.descargar_csv(enlace_csv)
                if ruta_csv:
                    self.guardar_fecha_actualizacion(anio, fecha_actual)
                return ruta_csv
            else:
                print(f"No hay actualización nueva de datos para el año {anio}.")
                return None
        except Exception as e:
            print(f"error al obtener csv en {url}: {e}")
            return None
        finally:
            driver.quit()

    def busqueda(self):
        driver = self.configuracion()
        try:
            driver.get("https://datos.gob.cl/dataset/registro-de-empresas-y-sociedades/resource/fd2b91b0-eb8e-45f1-98d0-1f3316bb6468")
            elemento_ul = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".list-unstyled.nav.nav-simple"))
            )

            urls_csv = []
            for li in elemento_ul.find_elements(By.TAG_NAME, "li"):
                enlace = li.find_element(By.TAG_NAME, "a")
                url = enlace.get_attribute("href")
                print(f"url del año encontrada: {url}")
                #para extrae el año del texto o del enlace
                anio_match = re.search(r"(\d{4})", enlace.text)
                anio = int(anio_match.group(1)) if anio_match else None
                if anio:
                    csv_url = self.navegacion(url, anio)
                    if csv_url:
                        urls_csv.append(csv_url)
            return urls_csv

        except Exception as e:
            print(f"Error al buscar los enlaces de años: {e}")
            return []

        finally:
            driver.quit()

    def registrar_revision(fuente):
        cliente = MongoClient("mongodb://localhost:27017/")
        db = cliente["DatosEmpresas"]
        db["revisiones"].update_one(
            {"fuente": fuente},
            {"$set": {"fecha_revision": datetime.now()}},
            upsert=True
        )
        cliente.close()

if __name__ == "__main__":
    scraper = ScraperDatosGob()
    scraper.busqueda()
    scraper.registrar_revision("datosgob")