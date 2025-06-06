from flask import Flask, Blueprint, jsonify, request, render_template
from dotenv import load_dotenv
from db import conexion_base_datos
from db_origen import conexion_base_datos_origen
import requests
import os
from dotenv import load_dotenv
import unicodedata
import re


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

"----------------------------------------------------------------------------------------------------------------"

# Endopoint para que Trantor envie informacion de empresas 
@empresas.route("/empresa/subir_info/", methods=["POST"])
def subir_info_empresa():
    datos = request.get_json()
    coleccion = db["EmpresasRevisadas"]
    campos_faltantes = []
    
    # para cargar las variables de entorno desde el archivo .env
    load_dotenv()
    api_token = os.getenv("API_TOKEN")

    # para validar los campos obligatorios y que tengan datos
    for campo in ["rut", "razon_social", "guardar_rutificador", "data"]:
        if campo not in datos:
            campos_faltantes.append(campo)
        elif campo != "guardar_rutificador" and not datos[campo]:
            campos_faltantes.append(campo)
    if campos_faltantes:
        return jsonify({"codigo": 400 ,
                        "estado": "Error, sintaxis incorrecta o datos invalidos",
                        "mensaje":f"Faltan campos obligatorios: {','.join(campos_faltantes)}"}), 400
    
    # para validar que el campo de 'data' tenga al menos una clave
    if not isinstance(datos["data"], dict) or not datos["data"]:
        return jsonify({"codigo": 422 ,
                        "estado": "Error en la semantica", 
                        "mensaje":f"Se necesita minimo una clave en el campo data: {','.join(datos['data'].keys())}"}), 422

    # para normalizar el rut
    rut = datos["rut"].replace(".", "").replace(" ", "").strip()

    # para validar el formato del rut
    if not re.match(r'^\d{7,8}-[\dkK]$', rut):
        return jsonify({"codigo": 400 ,
                        "estado": "Error, sintaxis incorrecta o datos invalidos",
                        "mensaje":"El formato del RUT es incorrecto."}), 400

    # para validar el formato de guardar_rutificador
    if not isinstance(datos["guardar_rutificador"], bool):
        return jsonify({"codigo":400, 
                        "estado": "Sintaxis incorrecta o datos invalidos",
                        "mensaje": "El campo 'guardar_rutificador' debe ser un booleano"
                        }), 400
    
    # para validar el token 
    token_recibido = request.headers.get("X-TOKEN")
    if not token_recibido or token_recibido != api_token:
        return jsonify({"codigo":401,
                        "estado": "No autorizado",
                        "mensaje": "Token invalido o no enviado"}), 401
    
    # Buscar datos previos en la colección empresas
    empresa = db["empresas"].find_one({"rut": rut})
    datos_previos = None
    if empresa:
        empresa["_id"] = str(empresa["_id"])
        datos_previos = empresa

    doc = {
        "rut": rut,
        "razon_social": datos["razon_social"],
        "guardar_rutificador": datos["guardar_rutificador"],
        "data": datos["data"]
    }

    try:
        if datos["guardar_rutificador"]:
            coleccion.insert_one(doc)
            mensaje = "La información fue guardada exitosamente"
        else:
            mensaje = "La informacion fue recibida exitosamente pero no se guardo en la base de datos"
    except Exception as e:
        return jsonify({"codigo": 500,
                        "estado": "Error interno del servidor",
                        "mensaje": f"Error al insertar en la base de datos: {str(e)}"}), 500

    if datos_previos:
        return jsonify({"codigo": 200,
                        "estado": "OK",
                        "mensaje": mensaje,
                        "datos_previos": datos_previos}), 200
    else:
        return jsonify({"codigo": 200,
                        "estado": "OK",
                        "mensaje": f"{mensaje}. Se busco la empresa pero no se encontraron datos previos {rut}"}), 200

 