from flask import Flask, Blueprint, jsonify, request, render_template
from dotenv import load_dotenv
from db import conexion_base_datos
from db_origen import conexion_base_datos_origen
import requests
import os
import unicodedata
import re
import datetime

db = conexion_base_datos().conexion() 
db_origen = conexion_base_datos_origen().conexion()


load_dotenv()  # Cargar las variables de entorno desde el archivo .env

# blueprints es para organizae los modulos de los endpoints
# jsonify es para convertir los datos a formato json, para que los entienda el frontend
# request es para obtener los datos que envia el frontend
# db es la conexion con la base de datos

TURNSTILE_SECRET_KEY =  os.getenv("TURNSTILE_SECRET_KEY")
if not TURNSTILE_SECRET_KEY:
   print(dict(os.environ))
   raise Exception("La variable de entorno TURNSTILE_SECRET_KEY no está definida.")

empresas = Blueprint("empresas", __name__) # Crear un grupo de endpoints para las empresas

@empresas.route("/empresa/buscar", methods=["GET"])
def buscar_empresa():

    #  Validar CAPTCHA 
    token = request.args.get('cf-turnstile-response')  # Turnstile manda esto

    if not token:
        return render_template("resultados.html", resultados=None, mensaje="Debes resolver el Captcha.")

    url = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
    data = {
        'secret': TURNSTILE_SECRET_KEY,
        'response': token
    }

    try:
       headers = {'Content-Type': 'application/x-www-form-urlencoded'}
       resp = requests.post(url, data=data, headers=headers)
        
       print("Token recibido:", token)
       print("Respuesta de Turnstile:", resp.text)


       # Verificar que la respuesta sea JSON
       if 'application/json' not in resp.headers.get('Content-Type', ''):
          return render_template("resultados.html", resultados=None, mensaje="Respuesta inválida del servidor CAPTCHA.")
 
       resultado = resp.json()
       if not resultado.get("success"):
           return render_template("resultados.html", resultados=None, mensaje="Captcha invalido. Intenta de nuevo.")
    except Exception as e:
        return render_template("resultados.html", resultados=None, mensaje="Error al verificar captcha.")

    valor = request.args.get('valor')

    if not valor:
        return render_template("resultados.html", resultados=None, mensaje="Debes ingresar un valor para buscar.")

    resultados = buscar_en_base_datos(valor)

    revision = db_origen["revisiones"].find_one({"fuente": "datosgob"})
    fecha_revision = revision["fecha_revision"] if revision else None

    if resultados:
        return render_template("resultados.html", resultados=resultados, fecha_revision=fecha_revision)
    return render_template("resultados.html", resultados=None, mensaje="no se encontraron resultados.",fecha_revision=fecha_revision)

def buscar_en_base_datos(valor):
    resultados_totales = []
    try:
        if not (3 <= len(valor) <= 100):
            raise ValueError("El valor debe tener entre 3 y 100 caracteres.")

        rut_pattern = re.compile(r'^\d{7,8}-[\dkK]$')
        valor_normalizado = normalizar_rut(valor)

        if rut_pattern.match(valor_normalizado):
            filtro = {"rut": valor_normalizado}
        else:
            valor_escapado = re.escape(valor)
            filtro = {"tags2": {"$regex": valor_escapado, "$options": "i"}}

    except Exception as e:
        print(f"Error al construir el filtro: {e}")
        return None

    try:
        coleccion = db["empresas"]
        resultados = list(coleccion.find(filtro).limit(10))  # Limitar resultados
        print(f"Buscando en EmpresasRevisadas - encontrados: {len(resultados)}")
        for resultado in resultados:
            resultado["_id"] = str(resultado["_id"])
            resultado_normalizado = {key.lower(): value for key, value in resultado.items()}
            resultados_totales.append(resultado_normalizado)

        return resultados_totales

    except Exception as e:
        print(f"Error al buscar: {e}")
        return None

    
def normalizar_rut(rut):
    return rut.replace(".", "").replace(" ", "").strip()

# def normalizar_razon_social(razon_social):
#     razon_social = "".join(
#         c for c in unicodedata.normalize("NFD", razon_social.lower())
#         if unicodedata.category(c) != "Mn"
#     ) 
#     return razon_social.replace("_", " ") 

"----------------------------------------------------------------------------------------------"


@empresas.route("/empresa/subir_info/", methods=["POST"])
def subir_info_empresa():
    datos = request.get_json()
    coleccion = db["empresas"]
    campos_faltantes = []

    try:
        for campo in ["rut", "razon_social"]:
            if campo not in datos or not datos[campo]:
                campos_faltantes.append(campo)
        
        if campos_faltantes:
            return jsonify({"error": f"faltan campos: {', '.join(campos_faltantes)}"}), 400

        razon_social = datos["razon_social"]
        rut = datos["rut"]

        historia = datos.get("historia", {})
        historia.setdefault("razon_social", [{
            "razon_social": razon_social,
            "vigente": True,
            "origen": "manual",
            "fecha_actualizacion": datetime.now().strftime("%d-%m-%Y")
        }])

        tags = [razon_social]
        tags2 = razon_social

        doc = {
            "rut": rut,
            "tags": tags,
            "tags2": tags2,
            "fecha_subida_datos": datetime.now().strftime("%Y-%m-%d"),
            "historia": historia,
            "socios": datos.get("socios", []),
            "representantes_legales": datos.get("representantes_legales", []),
            "direcciones": datos.get("direcciones", []),
            "actividades_economicas": datos.get("actividades_economicas", []),
            "actuacion": datos.get("actuacion", [])
        }

        resultado = coleccion.insert_one(doc)
        return jsonify({"Listo!": "Info agregada!"})

    except Exception as e:
        return jsonify({"error": str(e)})


