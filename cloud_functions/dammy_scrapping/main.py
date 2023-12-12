from firebase_functions import https_fn
from firebase_admin import initialize_app
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import requests
initialize_app()
import json

@https_fn.on_request(max_instances=10)
def dammy_scrapping(req: https_fn.Request) -> https_fn.Response:
    try:
        cred = 'config/lucaplugs-dev-sa.json'
        scopes=['https://www.googleapis.com/auth/spreadsheets']
        # Configuración de autenticación con la cuenta de servicio
        credents = service_account.Credentials.from_service_account_file(cred, scopes = scopes)

        # Crear un cliente de Google Sheets
        sheets_service = build('sheets', 'v4', credentials=credents)
        # Especificar la hoja de cálculo a la cual deseas acceder    
        spreadsheet_id = '1Nd5Zjtbcmn6Xx3O-Nt6k4vYZuKjK2HPhewBJwmqwy7o'
        # Rango de celdas que deseas leer (por ejemplo, 'Sheet1!A1:B10')
        range_ = 'Stage_AgeRet_DJ1948!A:AV'
        # Llamada a la API de Google Sheets para obtener los valores
        result = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_).execute()
            # Obtener los valores de las celdas
        values = result.get('values', [])
        df  = pd.DataFrame(values)
        nuevos_nombres = df.iloc[0]  # Obtener la primera fila como nombres de columna
        df = df[1:]  # Eliminar la primera fila (los nombres de las columnas actuales)
        df.columns = nuevos_nombres
        df = df.drop('TAG', axis=1)
        df = df.drop('ITER_ID', axis=1)
        
        df.insert(1, 'ANO', 2023)
        df.insert(2, 'MES', 5)
        df.insert(3, 'DIA', 12)

        
        
        
        grouped_df = df.groupby('TYPE')
        data_frames = []
        for _, group in grouped_df:
                group.reset_index(drop=True, inplace=True) #Reiniciamos los index del los df para que partan de  0
                data_frames.append(group.copy()) #Agregar cada grupo a la lista como un df indenpendiente 
        
        for df in data_frames:
            first_row = df.iloc[0]
            if first_row["TYPE"] == 'detalle':
                nombres_columnas =  [
                'ANIO',
                'MES',
                'DIA',
                'COL01',
                'COL02',
                'COL03',
                'COL04',
                'COL05',
                'COL06',
                'COL07',
                'COL08',
                'COL09',
                'COL10',
                'COL11',
                'COL12',
                'COL13',
                'COL14',
                'COL15',
                'COL16',
                'COL17',
                'COL18',
                'COL19',
                'COL20',
                'COL21',
                'COL22',
                'COL23',
                'COL24',
                'COL25',
                'COL26',
                'COL27',
                'COL28',
                'COL29',
                'COL30',
                'COL31',
                'COL32',
                'COL33',
                'COL34',
                'COL35',
                'COL36',
                'COL37',
                'COL38',
                'COL39',
                'COL40',
            ]
                
                # Definir la posición en la que se insertará la nueva columna
                posicion = 0  # En este caso, se insertará en la cuarta posición (índice 3)
                # Insertar la nueva columna en la posición definida
                df = df.drop("TYPE", axis=1)
                df = df.drop("REGID", axis=1)


                # Seleccionar las tres columnas que deseas juntar
               
                columnas_a_juntar = ["COL03", "COL04", "COL05"]
                df['rut_declarante'] = df[columnas_a_juntar].apply(lambda row: ''.join(map(str, row)), axis=1)
                #Eliminar las columnas seleccionadas originalmente
                df = df.drop(columnas_a_juntar, axis=1)
                df.insert(5, 'rut_declarante', df.pop('rut_declarante'))

                columnas_a_juntar = ["COL09", "COL10", "COL11"]
                df['rut_propietario'] = df[columnas_a_juntar].apply(lambda row: ''.join(map(str, row)), axis=1)
                #Eliminar las columnas seleccionadas originalmente
                df = df.drop(columnas_a_juntar, axis=1)
                df.insert(9, 'rut_propietario', df.pop('rut_propietario'))
                df['COL01'] = pd.to_numeric(df['COL01'])
                df['COL02'] = pd.to_numeric(df['COL02'])

                df.columns = nombres_columnas
                df_dammy = df.head(30)
                json_df = df_dammy.to_dict(orient='records')

                url = 'https://us-central1-luca-app-dev.cloudfunctions.net/core_v1_insert_data/insert_lote/dj1948_detalle'
                response = requests.post(url, json=json_df)
                if response.status_code == 200:
                    print('Solicitud exitosa!')
                    return https_fn.Response("Exito al insertar los datos")
             
                else:
                    print('Error en la solicitud:')
                    return https_fn.Response("Error al isnertar los datos")
               
                
            elif first_row["TYPE"] == 'resumen':
                pass
        
    except Exception as error:
        print("error in dammy_scrapping", error)