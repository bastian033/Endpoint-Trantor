import subprocess
import sys

def ejecutar(nombre, comando):
    print(f"\n--- Ejecutando: {nombre} ---")
    resultado = subprocess.run(comando)
    if resultado.returncode != 0:
        print(f"Error ejecutando {nombre}, deteniendo orquestador")
        sys.exit(1)
    print(f"--- {nombre} finalizado correctamente ---\n")

if __name__ == "__main__":

    # PARA DATOSGOB
    ejecutar("Scraper DatosGob", ["python", "scrapers/scraper_datosgob.py"])

    ejecutar("Migracion DatosGob", ["python", "utils/migracionDG.py"])
    
#-----------------------------------------------------------------------------
    # PARA SII
    ejecutar("Migracion SII", ["python", "scrapers/scraper_sii.py"])

    ejecutar("Migracion SII", ["python", "utils/migracionSII.py"])

    # para las razones sociales
    ejecutar("Duplicados RS", ["python", "utils/duplicados_RS.py"])

    # para las actividades económicas
    ejecutar("duplicados actividades_economicas", ["python", "utils/duplicados_AE.py"])

    # para las direcciones
    ejecutar("Duplicados Direcciones", ["python", "utils/migracion_direcciones.py"])

    # para rellenar el campo tags2
    ejecutar("Rellenar tags2", ["python", "utils/tags2.py"])
    
    print("Orquestador DatosGob: Proceso completo.")