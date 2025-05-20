from colorama import Fore, Style
import pandas as pd
import json
import os
import re

base = os.path.dirname(os.path.abspath(__file__))
cMap = pd.read_csv(os.path.join(base,'..', 'utils', 'cMappingTableActive.csv'), sep=';', dtype=str)
with open(os.path.join(base, '..', 'utils', 'sqlTypeMap.json'), 'r', encoding='utf-8') as f:
    sqlTypeMap = json.load(f)

def SQLSearch(cnxn: any, ids:str, limit:int, where:str, order:str, df: pd.DataFrame):
    #Query Data
    print(Fore.GREEN + f"Consultando el calculo - "+ Style.RESET_ALL + re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group() + Style.RESET_ALL)
    query = f"SELECT TOP({limit}) * FROM \"_Result{ids}\" {where} {order}"
    df = pd.read_sql(query, cnxn)
        
    df = df.drop(columns=['_ResultID'])

    concatFields = []

    # Obtenemos los tipos de datos de las columnas del DataFrame
    dtypes = df.dtypes
    for col in df.columns:
        # Verificamos si el tipo es string u object (para texto), o si el nombre de la columna es diferente de 'value'
        if (pd.api.types.is_string_dtype(dtypes[col]) or pd.api.types.is_datetime64_any_dtype(dtypes[col])) and col.lower() != 'value':
            concatFields.append(col)
    if concatFields:
        df['CONCAT'] = df[concatFields].astype(str).agg(''.join, axis=1)
    else:
        print(Fore.RED + "No se encontraron columnas para concatenar" + Style.RESET_ALL)
        df['CONCAT'] = None
    return df

def SQLEmpty(cnxn: any, URLid: list, where: str):
    #Creamos una lista de subconsultas para cada tabla en URLid
    subconsultas = [f"(SELECT TOP 1 1 FROM _Result{tabla} {where}) AS '{tabla}'" for tabla in URLid]

    #Generamos el query con la lista de subconsultas
    query = f"SELECT {', '.join(subconsultas)}"

    #Ejecutamos el query y almacentamos el resultado en un DataFrame
    df = pd.read_sql(query, cnxn)

    #Copiamos el DataFrame para imprimir los registros que no tienen datos
    empty_list = df.loc[:, df.isna().all()].columns.tolist()
    print(Fore.RED + f"No se encontraron datos en los periodos especificados para los siguientes calculos:" + Style.RESET_ALL)
    for ids in empty_list:
        print(re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group())

    # Eliminamos las columnas que no tienen datos
    df = df.dropna(axis=1, how='all')

    # Hacemos una lista con las columnas que tienen datos para posteriormente solo buscar en esas tablas 
    URLid = df.columns.tolist()

    return URLid