from firebase_functions import https_fn
from flask import Flask, request
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import os

app = Flask("internal")
cred = credentials.ApplicationDefault()
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
    from datetime import datetime
    try:
        """
            Obtener los registros por año y guardarlos en firestore
        """

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

        
        return {"res": "Exito al Actualizar el IPC"}
    except Exception as ex:
        print(f"error!!: {str(ex)}")
        return {"res":str(ex)}


# @app.route('/get_ipc', method=["GET"])
# def get_ipc():
#     try:
#         pass
#     except Exception as error:
#         print("Error in get_ipc()", error)