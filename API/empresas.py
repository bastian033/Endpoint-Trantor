from flask import Flask, Blueprint, jsonify, request, render_template
from db import db

# blueprints es para organizae los modulos de los endpoints
# jsonify es para convertir los datos a formato json, para que los entienda el frontend
# request es para obtener los datos que envia el frontend
# db es la conexion con la base de datos

empresas = Blueprint("empresas", __name__) # Crear un grupo de endpoints para las empresas

@empresas.route("/empresa/buscar", methods=["GET"])
def buscar_empresa():
    tipo_busqueda = request.args.get('tipo_busqueda')
    valor = request.args.get('valor')

    if not tipo_busqueda or not valor:
        return render_template("resultados.html", resultados=None, mensaje="Debe seleccionar un tipo de búsqueda y proporcionar un valor.")

    filtro = {}
    if tipo_busqueda == "razon_social":
        filtro["Razon Social"] = {"$regex": valor, "$options": "i"}  # Búsqueda insensible a mayúsculas
    elif tipo_busqueda == "rut":
        valor = normalizar_rut(valor)
        filtro["RUT"] = valor
    else:
        return render_template("resultados.html", resultados=None, mensaje="Tipo de búsqueda no válido.")

    resultados = buscar_en_base_datos(filtro)

    if resultados:
        return render_template("resultados.html", resultados=resultados)
    return render_template("resultados.html", resultados=None, mensaje="No se encontraron resultados.")

def buscar_en_base_datos(filtro):
    colecciones = [f"DatosGob{anio}" for anio in range(2013, 2026)]
    resultados_totales = []

    for coleccion_nombre in colecciones:
        coleccion = db[coleccion_nombre]
        resultados = list(coleccion.find(filtro))
        for resultado in resultados:
            resultado["_id"] = str(resultado["_id"])  # convertir ObjectId a string
            resultados_totales.append(resultado)

    return resultados_totales

def normalizar_rut(rut):
    """Normalizar el RUT eliminando puntos y espacios."""
    return rut.replace(".", "").replace(" ", "").strip()