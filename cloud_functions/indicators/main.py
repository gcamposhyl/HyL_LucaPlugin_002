from firebase_functions import https_fn
from flask import Flask, request
import firebase_admin
from google.oauth2 import service_account
from googleapiclient.discovery import build
from firebase_admin import credentials
from firebase_admin import firestore
from datetime import datetime
import pandas as pd
import os
import requests
from dotenv import load_dotenv
load_dotenv()

app = Flask("internal")
#cred = credentials.ApplicationDefault()
cred = credentials.Certificate('config/lucaplugs-sa.json')
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

@app.route('/update_factor_ipc', methods=['GET'])
def update_factor_ipc():
    try:
        cred = 'config/lucaplugs-dev-sa.json'
        scopes=['https://www.googleapis.com/auth/spreadsheets']
        # Configuración de autenticación con la cuenta de servicio
        credents = service_account.Credentials.from_service_account_file(cred, scopes = scopes)

        # Crear un cliente de Google Sheets
        sheets_service = build('sheets', 'v4', credentials=credents)
        # Especificar la hoja de cálculo a la cual deseas acceder    
        spreadsheet_id = '1V6SkxT8I_DqdLVdhQYZX5ojFYuIJzpE2drp2w9uKi_k'
        # Rango de celdas que deseas leer (por ejemplo, 'Sheet1!A1:B10')
        range_ = 'Correccion Monetaria Mensual!A:D'
        # Llamada a la API de Google Sheets para obtener los valores
        result = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_).execute()
            # Obtener los valores de las celdas
        values = result.get('values', [])
        
        df = pd.DataFrame(values)
        del values
        del result
        del range_
        del spreadsheet_id
        del credents
        del scopes
        del cred
        nuevas_columnas = df.iloc[0]  # Obtener la primera fila
        df = df[1:]  # Excluir la primera fila del DataFrame
        df.columns = nuevas_columnas  # Establecer las nuevas columnas

        list_df = []
        df_sin_columnas = df[["Anio", "Mes", "Factor IPC"]]
        group_df = df_sin_columnas.groupby('Anio')
        del df_sin_columnas
        del df
        for _, group in group_df:
            group.reset_index(drop=True, inplace=True) #Reiniciamos los index del los df para que partan de  0
            list_df.append(group.copy()) #Agregar cada grupo a la lista como un df indenpendiente 
        
        del group_df

        for df in list_df:
           
            year = df.iloc[0]["Anio"]
            url = f"https://us-central1-luca-app-dev.cloudfunctions.net/core_v1_operation_transactional/insert/factor_ipc?id_document={str(year)}"
            data = {
                "Capital Inicial": float(df.iloc[0]["Factor IPC"].replace(",",".")),
                "Enero":float(df.iloc[1]["Factor IPC"].replace(",",".")),
                "Febrero": float(df.iloc[2]["Factor IPC"].replace(",",".")),
                "Marzo": float(df.iloc[3]["Factor IPC"].replace(",",".")),
                "Abril":float(df.iloc[4]["Factor IPC"].replace(",",".")),
                "Mayo":float(df.iloc[5]["Factor IPC"].replace(",",".")),
                "Junio":float(df.iloc[6]["Factor IPC"].replace(",",".")),
                "Julio" :float(df.iloc[7]["Factor IPC"].replace(",",".")),
                "Agosto":float(df.iloc[8]["Factor IPC"].replace(",",".")),
                "Septiembre":float(df.iloc[9]["Factor IPC"].replace(",",".")),
                "Octubre":float(df.iloc[10]["Factor IPC"].replace(",",".")),
                "Noviembre": float(df.iloc[11]["Factor IPC"].replace(",",".")),
                "Diciembre": float(df.iloc[12]["Factor IPC"].replace(",","."))
            }
            response = requests.post(url, json=data)
            del response
        


            # for indice, fila in df.iterrows():
            #     year =  fila["Anio"]
                # factor_ipc = fila["Factor IPC"]
                # url = f"https://us-central1-luca-app-dev.cloudfunctions.net/core_v1_operation_transactional/update/{str(year)}?collection=factor_ipc"
                # data = {
                #         fila["Mes"]:float(factor_ipc.replace(",", "."))
                #     }
                # del year
                # response = requests.post(url, json=data)
                # del data
                # del response

        return {"res": "Factor de IPC actualizado"}
    except Exception as error :
        print("error in update_factor_ipc()", error)

# #insertar mesaje desde plugin en cola core
# @app.route('/update_ipc_list', methods=['GET'])
# def update_ipc():
#     import json
#     import requests
   
#     try:
#         """
#             Obtener los registros por año y guardarlos en firestore
#         """
        
#         is_empty = collection_empty()
#         #Si la collection se encuentra vacia
#         if is_empty:
#             current_year = datetime.now().year
#             firt_year = current_year-10
            
#             for year in range(firt_year, current_year +1):
#                 url = f'https://mindicador.cl/api/ipc/{str(year)}'
#                 response = requests.get(url)
#                 if response.status_code == 200:
#                     data = response.json()
#                     list_ipc_for_month = data["serie"] 
#                     doc_ref = db.collection("ipc").document(str(year))
#                     list_months = ["Diciembre", "Noviembre", "Octubre", "Septiembre", "Agosto", "Julio", "Junio", "Mayo", "Abril", "Marzo", "Febrero", "Enero"]
                    
#                     url = f"{os.getenv('URL')}/core_v1_operation_transactional/insert/ipc?id_document={str(year)}"
#                     response = requests.post(url)
                

#                     index_initial = 12 - len(list_ipc_for_month)
#                     for i in range(index_initial, len(list_ipc_for_month) + index_initial):
#                         url = f"{os.getenv('URL')}/core_v1_operation_transactional/update/{str(year)}?collection=ipc"
#                         data = {
#                             list_months[i]:list_ipc_for_month[i - index_initial]["valor"]
#                         }
#                         response = requests.post(url, json=data)
#                 else:
#                     print("Error al realizar la solicitud!!!!!!!!!!!!!", response.status_code)
#                     return {"res": "Problemas al conectarse con la Api de mindicador.cl"}
#             return {"res": "Lista de IPC cargada con exito"}
        
#         else:
#             print("existen registros en la coleccion")
#             update_collection = update_last_month()
#             return {"res": update_collection}
#     except Exception as ex:
#         print(f"error!!: {str(ex)}")
#         return {"res":str(ex)}


# @app.route('/get_ipc', methods=["GET"])
# def get_ipc():
#     """
#         Obtener IPC POR AÑO, ENTREGANDO COMO RESULTADO UN ARRAY CON LOS REGISTROS
    
#     Args:
#         year: Number

#     Return:
#         List_ipc : Array
    
#     """
#     try:
#         year = request.args.get('year')
#         doc_ref = db.collection("ipc").document(str(year))
#         doc = doc_ref.get()
#         if doc.exists:
#             print(f"Document data: {doc.to_dict()}")
#             return {"res":doc.to_dict()} 
#         else:
#             return {"res": f"El Año {year} no se encuentra en los registrados"}
#     except Exception as error:
#         print("Error in get_ipc()", error)


# @app.route('/get_one_ipc', methods=["GET"])
# def get_one_ipc():
#     try:
#         year = request.args.get("year")
#         month = request.args.get("month")
#         doc_ref = db.collection("ipc").document(str(year))
#         doc = doc_ref.get()
#         if doc.exists:
#             register = doc.to_dict()
#             if month in register:
#                 return {"res": register[month]}
#             else:
#                return {"res": "El IPC solicitado no existe"} 
#         else:
#             return {"res": "El Año solicitado no se encuentra en los registros"}
#     except Exception as error:
#         print("Error in get_one_ipc()", error)


# def collection_empty():
#     try:
#         """
#             Identificar si la coleccion de firestore tiene registros 

#             Args:
#                 coleccion: String
#             return:
#                 is_docs_empty: boolean
#         """
#         url = f"{os.getenv('URL')}/core_v1_data_api/getAllIpc"
#         response = requests.get(url)
#         if response.status_code == 200:
#             data = response.json()
#             if len(data["ipc"]) == 0:
                
#                 is_docs_empty = True
#             else:
#                 is_docs_empty = False
#         else:
#             print("Error interno del core en core_v1_data_api/getAllIpc")

#         return is_docs_empty
        

#     except Exception as error:
#         print("Error in get_collection()", error)


# def update_last_month():
#     """
#         Actualizar el ultimo mes mediante la Api

#         Identificar el año actual
#         identificar el año actual en la base de datos
#         actualizar los registros del año
#     """
#     try:
#         current_year = datetime.now().year
#         url = f'https://mindicador.cl/api/ipc/{str(current_year)}'
#         response = requests.get(url)
        
#         #Peticion HTTP a interfaz del core
#         url_firetore  = f"{os.getenv('URL')}/core-v1-get_data_interface/getAllByYear?year={str(current_year)}"
#         response_firestore = requests.get(url_firetore)
#         if response_firestore.status_code == 200:
#             data_firestore = response_firestore.json()
#             doc = data_firestore["ipc"]                 #Campos del documento de IPC

#         list_months = ["Diciembre", "Noviembre", "Octubre", "Septiembre", "Agosto", "Julio", "Junio", "Mayo", "Abril", "Marzo", "Febrero", "Enero"]
        
#         if response.status_code == 200:
#             data = response.json()
#             insert_ipc = insert_new_ipc(data, doc, list_months, current_year)
            
#             return insert_ipc
#         else:
#             print("Activando metodo de scrapping")
#             data = scrapping_sii()
#             insert_ipc = insert_new_ipc(data, doc, list_months, current_year)

#             return insert_ipc
            
#     except Exception as error:
#         print("error in update_last_month()", error)


# def scrapping_sii():
#     try:
#         """
#             Realizar scrapping con xpath de  una tabla de ipc del año la cual se encuentra en la pagina de SII

#             Args:
#                 None
            
#             Return:
#                Array 
#         """
#         import requests
#         from lxml import html

#         url = "https://www.sii.cl/valores_y_fechas/utm/utm2023.htm"
#         response = requests.get(url)
#         meses = [
#             'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
#             'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
#         ]

#         if response.status_code == 200: #Si el acceso a la url es exitoso
#             html_content = response.content
#             parsed_document = html.fromstring(html_content)  # Parsear (transformar datos) contenido HTML
#             table_elements = parsed_document.xpath("//*[contains(@class, 'table-responsive')]//table") #Obtener la tabla que se encuentra en la clase table-responsive 

          
#             list_month_with_ipc = []

#             # Si se encuentra una tabla dentro de 'table-responsive'
#             if table_elements:
#                 table = table_elements[0]  # Obtener la primera tabla encontrada
#                 tbody = table.xpath(".//tbody") #obtener el tbody de la tabla

#                 # Si se encuentra un elemento tbody
#                 if tbody:
#                     rows = tbody[0].xpath(".//tr") #obtener los registros con etiqueta tr

#                     for index, row in enumerate(rows):
#                         columns = row.xpath(".//td//text()") #obtener el texto que se encuentra dentro de las etiquetas td
#                         columns = [column.strip() for column in columns] #guarda la informacion en array

#                         if columns[3] == "":#Si en la tercera posicion del array  esta vacia
#                             continue    #pasa al siguente registro

                        
#                         numero_str = columns[3]                     # Cadena que representa el número en formato de coma flotante
#                         numero_str = numero_str.replace(',', '.')   # Reemplazar la coma por un punto decimal si es necesario
#                         numero_float = float(numero_str)            # Convertir la cadena a un número decimal (float)

#                         list_month_with_ipc.append({"fecha": meses[index], "valor":numero_float})

#             inverted_list = list(reversed(list_month_with_ipc)) #invertir fila para que el orden sea de
#         return {"serie": inverted_list}
#     except Exception as error:
#         print("Error in scraping_sii", error)


# def insert_new_ipc(data, doc, list_months, current_year):
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
        list_ipc_for_month = data["serie"]                      #datos obtenidos de la api de ipc
        if len(list_ipc_for_month) > len(doc):
            index_initial = 12 - len(list_ipc_for_month)
            for i in range(index_initial, len(list_ipc_for_month) + index_initial):
                url = f'{os.getenv("URL")}/core_v1_operation_transactional/update/{str(current_year)}?collection=ipc'
                data = {
                    list_months[i]:list_ipc_for_month[i - index_initial]["valor"]
                }
                response = requests.post(url, json=data)
                if response.status_code == 200:
                   
            #     doc_ref.update({
            #     list_months[i]: list_ipc_for_month[i - index_initial]["valor"]
            # })
                    return "Lista de IPC actualizada con exito"
                else:
                    return "Error interno en la funcion de core_v1_operation_transactional"
            
        else:
            return f"La lista de IPC del año {current_year} ya se encuentra actualizada"
    except Exception as error:
        print("insert_new_ipc()", error)