from colorama import Fore, Style
import pandas as pd
import os
import re

base = os.path.dirname(os.path.abspath(__file__))
cMap = pd.read_csv(os.path.join(base,'..', 'utils', 'cMappingTableActive.csv'), sep=';', dtype=str)

def SQLSearch(cnxn: any, ids:str, where:str, order:str, df: pd.DataFrame):
    #Query Data
    query = f"SELECT * FROM \"_Result{ids}\" {where} {order}"
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
    if empty_list.__len__() > 0:
        print(Fore.YELLOW + f"Los siguientes Calculos fueron descartados por no contener datos en los periodos especificados:" + Style.RESET_ALL)
        for ids in empty_list:
            print(f"{Fore.RED}{re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group()}")
        # Eliminamos los registros que no tienen datos
        df = df.dropna(axis=1, how='all')
    
    # Hacemos una lista con las columnas que tienen datos para posteriormente solo buscar en esas tablas 
    URLid = df.columns.tolist()
    #Imprimimos los calculos que se consultaran
    print("\n" + Fore.GREEN + "Se consultaran los siguientes Calculos: " + Style.RESET_ALL)
    # for ids in URLid:
        # print(f"{Fore.BLUE}{re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group()}{Style.RESET_ALL}")

    return URLid