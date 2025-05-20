from colorama import Fore, Style
import requests as rq
import pandas as pd 
import json
import os
import re

cMap_path = os.path.join(os.path.dirname(__file__), '..', 'utils', 'cMappingTableActive.csv')
sqlTypeMap = os.path.join(os.path.dirname(__file__), '..', 'utils', 'sqlTypeMap.json')
cMap = pd.read_csv(os.path.abspath(cMap_path), sep=';', dtype=str)

def getPayload(id: str, where: str, order: str):
    data = {
        "queryString": f"SELECT * FROM \"_Result{id}\" {where} {order}",
        "offset": 0,
        "limit": 0
    }
    return data

def getResponse(apiurl: any, header: any, data: any, ids: str):
    response = rq.post(apiurl, headers = header, json = data)
    if response.status_code == 200:
        print(re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group() + Fore.GREEN + " - Success! - " + Style.RESET_ALL + " Status Code: " + str(response.status_code))
    elif response.status_code == 401:
        print(re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group() + Fore.YELLOW + " - Unauthorized - " + Style.RESET_ALL + "Status Code: " + str(response.status_code))
    elif response.status_code == 403:
        print(re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group() + Fore.MAGENTA + " - Forbidden - " + Style.RESET_ALL + "Status Code: " + str(response.status_code))
    elif response.status_code == 404:
        print(re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group() + Fore.RED + " - Not Found - " + Style.RESET_ALL + "Status Code: " + str(response.status_code))
    elif response.status_code == 500:
        print(re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group() + Fore.RED + " - Internal Server Error - " + Style.RESET_ALL + "Status Code: " + str(response.status_code))
    else:
        print(Fore.RED + "Unknown Error - " + Style.RESET_ALL + "Status Code: " + str(response.status_code))
        print(response.status_code)
        print(response.text)
    return response

def construyeDataFrame(jResponse: any, df: pd.DataFrame):
    #Definimos cuales son las columnas y tipos de datos en jResponse
    columns = [col['name'] for col in jResponse['columnDefinitions'][0]]
    types = [col['type'] for col in jResponse['columnDefinitions'][0]]

    #Definimos un diccionario para los tipos de datos
    with open(sqlTypeMap, 'r') as f:
        type_map = json.load(f)

    #Definimos el DataFrame y sus tipos de datos
    type_dict = {col: type_map.get(typ.lower(), 'object') for col, typ in zip(columns, types)}

    #Llenamos el DataFrame con los datos de la respuesta
    df = pd.DataFrame(jResponse['data'][0], columns=columns).astype(type_dict)

    #Eliminamos la columna _ResultID
    df = df.drop(columns=['_ResultID'])

    #Definimos un arreglo que contiene los nombres de las columnas que se concatenaran
    concatFields = []

    #Recorremos las columnas para elegir los campos que se concatenaran
    for col in jResponse['columnDefinitions'][0]:
        if col['type'] in ['String', 'Date'] and col['name'].lower() != 'value':
            concatFields.append(col['name'])
    
    #Concatenamos los campos elegidos
    df['CONCAT'] = df[concatFields].astype(str).agg(''.join, axis=1)
    return df