import subprocess
import sys

def ejecutar(nombre, comando):
    print(f"\n--- Ejecutando: {nombre} ---")
    resultado = subprocess.run(comando)
    if resultado.returncode != 0:
        print(f"Error ejecutando {nombre}, Deteniendo orquestador.")
        sys.exit(1)
    print(f"--- {nombre} finalizado correctamente ---\n")

if __name__ == "__main__":

    # Primero para los datos de DatosGob

    # 1. para ejecutar el scraper
    ejecutar("Scraper DatosGob", ["python", "scrapers/scraper_datosgob.py"])
    
    # 2. para ejecutar la migracion 
    ejecutar("Migracion DatosGob", ["python", "utils/migracionDG.py"])
    
    # 3. para ejecutar la limpieza
    ejecutar("Limpieza DatosGob", ["python", "utils/limpiezaSII.py"]) 

    
    print("Orquestador DatosGob: Proceso completo.")