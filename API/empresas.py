from flask import Flask, Blueprint, jsonify, request, render_template
from dotenv import load_dotenv
from db import db
import requests
import os

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
        resp = requests.post(url, data=data)
        resultado = resp.json()
        if not resultado.get("success"):
            return render_template("resultados.html", resultados=None, mensaje="Captcha invalido. Intenta de nuevo.")
    except Exception as e:
       return render_template("resultados.html", resultados=None, mensaje="Error al verificar captcha.")

    tipo_busqueda = request.args.get('tipo_busqueda')
    valor = request.args.get('valor')

    if not tipo_busqueda or not valor:
        return render_template("resultados.html", resultados=None, mensaje="Debe seleccionar un tipo de busqueda y proporcionar un valor.")

    tipo_busqueda = request.args.get('tipo_busqueda')
    valor = request.args.get('valor')

    if not tipo_busqueda or not valor:
        return render_template("resultados.html", resultados=None, mensaje="Debe seleccionar un tipo de busqueda y proporcionar un valor.")

    filtro = {}
    if tipo_busqueda == "razon_social":
        filtro = { # Busqueda insensible a mayusculas
        "$or": [
            {"Razon Social": {"$regex": valor, "$options": "i"}},
            {"razon_social": {"$regex": valor, "$options": "i"}},
            {"razonSocial": {"$regex": valor, "$options": "i"}}
        ]
    }  
    elif tipo_busqueda == "rut":
        valor = normalizar_rut(valor)
        filtro = {
        "$or": [
            {"RUT": valor},
            {"rut": valor}
        ]
    }
    else:
        return render_template("resultados.html", resultados=None, mensaje="tipo de busqueda no valido.")

    resultados = buscar_en_base_datos(filtro)

    if resultados:
        return render_template("resultados.html", resultados=resultados)
    return render_template("resultados.html", resultados=None, mensaje="no se encontraron resultados.")

def buscar_en_base_datos(filtro):
    colecciones = db.list_collection_names()
    resultados_totales = []
    try:
        for coleccion_nombre in colecciones:
            coleccion = db[coleccion_nombre]
            resultados = list(coleccion.find(filtro))
            print(f"Buscando en {coleccion_nombre} - encontrados: {len(resultados)}")
            for resultado in resultados:
                resultado["_id"] = str(resultado["_id"])
                resultados_totales.append(resultado)

        return resultados_totales

    except Exception as e:
        print(f"Error al buscar: {e}")
        return None

def normalizar_rut(rut):
    return rut.replace(".", "").replace(" ", "").strip()


"----------------------------------------------------------------------------------------------"


@empresas.route("/empresa/subir_info/", methods=["POST"])
def subir_info_empresa():
    datos = request.get_json()
    coleccion = db["EmpresasRevisadas"]
    campos_faltantes = []

    try:
        for campo in ["RUT", "Razon Social"]:
            if campo not in datos:
                campos_faltantes.append(campo)
        
        if campos_faltantes:
            return jsonify({"error": f"faltan campos: {', '.join(campos_faltantes)}"}), 400

        resultado = coleccion.insert_one(datos)
        return jsonify({"Listo!": "Info agregada!" })

    except Exception as e:
        return jsonify({"error":e})


