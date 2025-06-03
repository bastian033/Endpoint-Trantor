from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import datetime

url = "https://www.sii.cl/sobre_el_sii/nominapersonasjuridicas.html"

driver = webdriver.Chrome()
driver.get(url)

try:
    div_contenido = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CLASS_NAME, "col-sm-9.contenido"))
    )

    # Encuentra el primer <ul> (el de las nóminas)
    ul = div_contenido.find_element(By.TAG_NAME, "ul")
    lis = ul.find_elements(By.TAG_NAME, "li")

    nombres_objetivo = [
        "Nómina de actividades económicas de contribuyentes personas jurídicas",
        "Nómina de direcciones históricas de contribuyentes personas jurídicas",
        "Nómina de Razón Social de contribuyentes personas jurídicas",
        "Nómina de empresas personas jurídicas año comercial 2020-2023",
        "Nómina de empresas personas jurídicas año comercial 2015-2019",
        "Nómina de empresas personas jurídicas año comercial 2010-2014",
        "Nómina de empresas personas jurídicas año comercial 2005-2009",
    ]

    # Obtén todos los hijos directos del div_contenido para buscar fechas externas
    hijos = div_contenido.find_elements(By.XPATH, "./*")

    # Busca la fecha general después del <ul>
    fecha_general = None
    for idx, hijo in enumerate(hijos):
        if hijo == ul:
            for siguiente in hijos[idx+1:]:
                # Busca el div con margin-left:40px (puedes buscar por class si la tuviera)
                if "margin-left:40px" in siguiente.get_attribute("outerHTML"):
                    try:
                        fecha_div = siguiente.find_element(By.CLASS_NAME, "fecha-actualizacion")
                        fecha_texto = fecha_div.text.strip()
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
                                fecha_general = datetime.date(anio, mes, 1)
                    except:
                        pass
                    break
            break

    for idx, li in enumerate(lis):
        # Busca el <a> dentro del <li>
        try:
            a = li.find_element(By.TAG_NAME, "a")
            nombre = a.text.strip()
        except:
            continue

        if not any(obj in nombre for obj in nombres_objetivo):
            continue

        fecha_actualizacion = None

        # Intenta encontrar la fecha dentro del <li>
        try:
            fecha_div = li.find_element(By.CLASS_NAME, "fecha-actualizacion")
            fecha_texto = fecha_div.text.strip()
        except:
            fecha_texto = None

        # Extrae la fecha si existe
        if fecha_texto:
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
                    fecha_actualizacion = datetime.date(anio, mes, 1)

        # Si no hay fecha específica y es una nómina año comercial, usa la fecha general
        if not fecha_actualizacion and "año comercial" in nombre and fecha_general:
            fecha_actualizacion = fecha_general

        print(f"{nombre} -> Fecha de actualización detectada: {fecha_actualizacion}")

finally:
    driver.quit()