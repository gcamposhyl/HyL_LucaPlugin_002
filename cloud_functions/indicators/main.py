from firebase_functions import https_fn
from flask import Flask, request
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from datetime import datetime
import os
import requests

app = Flask("internal")
cred = credentials.ApplicationDefault()
#cred = credentials.Certificate('config/lucaplugs-dev-sa.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

@https_fn.on_request(max_instances=10)
def plugin_v1_indicators_api_rest(request: https_fn.Request) -> https_fn.Response:
    """
        - Insertar en firestore todos los datos de los IPC de los ultimos 10 años
        - Crear documento donde la id sea el año y dentro cada field sera el mes con el monto
        - Crear 2 endoint 
            Actualizar lista de ipc
            uno para mostrar los registros por año 
            otro para mostrar el ipc de un año y mes especifico

            
    -Endpoind(/list_ipc_for_year):
            method: GET
            Args: year
    
    -Endpoind(/update_ipc):
                method: POST
                Args: None
    """

    with app.test_request_context(path=request.full_path, method=request.method):
        #Create a new app context for the internal app
            internal_ctx = app.test_request_context(path=request.full_path,
                                                    method=request.method)
            
            #Copy main request data from original request
            #According to your context, parts can be missing. Adapt here!
            internal_ctx.request.data = request.data
            internal_ctx.request.headers = request.headers
            
            #Activate the context
            internal_ctx.push()
            #Dispatch the request to the internal app and get the result 
            return_value = app.full_dispatch_request()
            #Offload the context
            internal_ctx.pop()
    
    # Return the result of the internal app routing and processing
    return return_value

#insertar mesaje desde plugin en cola core
@app.route('/update_ipc_list', methods=['GET'])
def update_ipc():
    import json
    import requests
   
    try:
        """
            Obtener los registros por año y guardarlos en firestore
        """
        
        is_empty = collection_empty("ipc")
        if is_empty:
            current_year = datetime.now().year
            firt_year = current_year-10
            
            for year in range(firt_year, current_year +1):
                
                url = f'https://mindicador.cl/api/ipc/{str(year)}'
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    list_ipc_for_month = data["serie"] 
                    doc_ref = db.collection("ipc").document(str(year))
                    list_months = ["Diciembre", "Noviembre", "Octubre", "Septiembre", "Agosto", "Julio", "Junio", "Mayo", "Abril", "Marzo", "Febrero", "Enero"]
                    
                    doc_ref.set({
                        
                    })
                    index_initial = 12 - len(list_ipc_for_month)
                    for i in range(index_initial, len(list_ipc_for_month) + index_initial):
                        doc_ref.update({
                        list_months[i]: list_ipc_for_month[i - index_initial]["valor"]
                        })
                else:
                    print("Error al realizar la solicitud!!!!!!!!!!!!!", response.status_code)
                    return {"res": "Problemas al conectarse con la Api de mindicador.cl"}
            return {"res": "Lista de IPC cargada con exito"}
        
        else:
            update_collection = update_last_month()
            return {"res": update_collection}
    except Exception as ex:
        print(f"error!!: {str(ex)}")
        return {"res":str(ex)}


@app.route('/get_ipc', methods=["GET"])
def get_ipc():
    """
        Obtener IPC POR AÑO, ENTREGANDO COMO RESULTADO UN ARRAY CON LOS REGISTROS
    
    Args:
        year: Number

    Return:
        List_ipc : Array
    
    """
    try:
        year = request.args.get('year')
        doc_ref = db.collection("ipc").document(str(year))
        doc = doc_ref.get()
        if doc.exists:
            print(f"Document data: {doc.to_dict()}")
            return {"res":doc.to_dict()} 
        else:
            return {"res": f"El Año {year} no se encuentra en los registrados"}
    except Exception as error:
        print("Error in get_ipc()", error)


@app.route('/get_one_ipc', methods=["GET"])
def get_one_ipc():
    try:
        year = request.args.get("year")
        month = request.args.get("month")
        doc_ref = db.collection("ipc").document(str(year))
        doc = doc_ref.get()
        if doc.exists:
            register = doc.to_dict()
            if month in register:
                return {"res": register[month]}
            else:
               return {"res": "El IPC solicitado no existe"} 
        else:
            return {"res": "El Año solicitado no se encuentra en los registros"}
    except Exception as error:
        print("Error in get_one_ipc()", error)


def collection_empty(collection):
    try:
        """
            Identificar si la coleccion de firestore tiene registros 

            Args:
                coleccion: String
            return:
                is_docs_empty: boolean
        """
        docs = db.collection(collection).stream()
        is_docs_empty = True
        for doc in docs:  
            is_docs_empty = False
        
        return is_docs_empty
        

    except Exception as error:
        print("Error in get_collection()", error)


def update_last_month():
    """
        Actualizar el ultimo mes mediante la Api

        Identificar el año actual
        identificar el año actual en la base de datos
        actualizar los registros del año
    """
    try:
        current_year = datetime.now().year
        url = f'https://mindicador.cl/api/ipc/{str(current_year)}'
        response = requests.get(url)
        
        doc_ref = db.collection("ipc").document(str(current_year))
        doc = doc_ref.get()

        list_months = ["Diciembre", "Noviembre", "Octubre", "Septiembre", "Agosto", "Julio", "Junio", "Mayo", "Abril", "Marzo", "Febrero", "Enero"]
        
        if response.status_code == 200:
            print("Entre al if")
            data = response.json()
            insert_ipc = insert_new_ipc(data, doc, doc_ref, list_months, current_year)
            
            return insert_ipc
        else:
            
            data = scrapping_sii()
            insert_ipc = insert_new_ipc(data, doc, doc_ref, list_months, current_year)

            return insert_ipc
            
    except Exception as error:
        print("error in update_last_month()", error)


def scrapping_sii():
    try:
        """
            Realizar scrapping con xpath de  una tabla de ipc del año la cual se encuentra en la pagina de SII

            Args:
                None
            
            Return:
               Array 
        """
        import requests
        from lxml import html

        url = "https://www.sii.cl/valores_y_fechas/utm/utm2023.htm"
        response = requests.get(url)
        meses = [
            'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
            'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
        ]

        if response.status_code == 200: #Si el acceso a la url es exitoso
            html_content = response.content
            parsed_document = html.fromstring(html_content)  # Parsear (transformar datos) contenido HTML
            table_elements = parsed_document.xpath("//*[contains(@class, 'table-responsive')]//table") #Obtener la tabla que se encuentra en la clase table-responsive 

          
            list_month_with_ipc = []

            # Si se encuentra una tabla dentro de 'table-responsive'
            if table_elements:
                table = table_elements[0]  # Obtener la primera tabla encontrada
                tbody = table.xpath(".//tbody") #obtener el tbody de la tabla

                # Si se encuentra un elemento tbody
                if tbody:
                    rows = tbody[0].xpath(".//tr") #obtener los registros con etiqueta tr

                    for index, row in enumerate(rows):
                        columns = row.xpath(".//td//text()") #obtener el texto que se encuentra dentro de las etiquetas td
                        columns = [column.strip() for column in columns] #guarda la informacion en array

                        if columns[3] == "":#Si en la tercera posicion del array  esta vacia
                            continue    #pasa al siguente registro

                        
                        numero_str = columns[3]                     # Cadena que representa el número en formato de coma flotante
                        numero_str = numero_str.replace(',', '.')   # Reemplazar la coma por un punto decimal si es necesario
                        numero_float = float(numero_str)            # Convertir la cadena a un número decimal (float)

                        list_month_with_ipc.append({"fecha": meses[index], "valor":numero_float})

            inverted_list = list(reversed(list_month_with_ipc)) #invertir fila para que el orden sea de
        return {"serie": inverted_list}
    except Exception as error:
        print("Error in scraping_sii", error)



def insert_new_ipc(data, doc, doc_ref, list_months, current_year):
    """
        insertar el ultimo registro de IPC en firestore
        Args:
            data: obj
            doc: obj
            doc_reg: obj
            list_months: Array

        return:
                String
    """
    
    try:
        
        list_ipc_for_month = data["serie"] 
        if len(list_ipc_for_month) > len(doc.to_dict()):
            index_initial = 12 - len(list_ipc_for_month)
            for i in range(index_initial, len(list_ipc_for_month) + index_initial):
                doc_ref.update({
                list_months[i]: list_ipc_for_month[i - index_initial]["valor"]
            })
            return "Lista de IPC actualizada con exito"
        else:
            return f"La lista de IPC del año {current_year} ya se encuentra actualizada"
    except Exception as error:
        print("insert_new_ipc()", error)