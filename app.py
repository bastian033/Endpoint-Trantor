from flask import Flask, render_template
from API.empresas import empresas  # importo el blueprint de empresas

app = Flask(__name__)

# Filtro personalizado para mostrar valores bonitos
def mostrar_valor(valor):
    if valor is None or valor == '' or str(valor).lower() == 'nan':
        return 'No disponible'
    return valor

app.jinja_env.filters['mostrar_valor'] = mostrar_valor


# registro el blueprint de empresas
# el prefijo de la url sera /api junto con /empresa/buscar 
app.register_blueprint(empresas, url_prefix="/api") 

app.template_folder = "templates"

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)