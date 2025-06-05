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

    # Primero para los datos de DatosGob

    ejecutar("Scraper DatosGob", ["python", "scrapers/scraper_datosgob.py"])

    ejecutar("Migracion DatosGob", ["python", "utils/migracionDG.py"])
    
    # Ahora para los datos de SII

    ejecutar("Migracion SII", ["python", "scrapers/scraper_sii.py"])

    ejecutar("Migracion SII", ["python", "utils/migracionSII.py"])
 
    
    print("Orquestador DatosGob: Proceso completo.")