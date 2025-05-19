from colorama import Fore, Style
import pyodbc as db
import pandas as pd
import os
import re

# cMap = pd.read_csv(r'../utils/cMappingTableActive.csv', sep=';', dtype=str)
cMap_path = os.path.join(os.path.dirname(__file__), '..', 'utils', 'cMappingTableActive.csv')
cMap = pd.read_csv(os.path.abspath(cMap_path), sep=';', dtype=str)

def SQLSearch(cursor: any, ids:str, limit:int, where:str, order:str, df: pd.DataFrame):
    #Query Data
    cursor.execute(f"SELECT TOP({limit}) * FROM \"_Result{ids}\" {where} {order}")
    rows = cursor.fetchall()
    for i in rows:
        df.loc[len(df)] = list(i)
        
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

def SQLEmpty(cursor: any, URLid: list, where: str):
    #Query Data
    array = []
    for ids in URLid:
        cursor.execute(f"SELECT * FROM \"_Result{ids}\" {where}")
        rows = cursor.fetchall()
        if len(rows) == 0:
            print(Fore.RED + "No se encontraron datos " + Style.RESET_ALL + "para el incentivo " + re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group())
            array.append(ids)
    return array

def SQLConnect(str: str):
    cnxn = db.connect(str)
    print(Fore.GREEN + "Conectado al servidor SQL" + Style.RESET_ALL)
    return cnxn

# def SQLDisconnect(cnxn, cursor):
#     print(Fore.YELLOW + "SQL Connection Closed" + Style.RESET_ALL)
#     cursor.close()
#     cnxn.close()

def Getheader(cursor: any, id: str):
    cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'_Result{id}\' ORDER BY ORDINAL_POSITION")
    rows = cursor.fetchall()
    if len(rows) == 0:
        print(Fore.RED + "No se encontró cabecera " + Style.RESET_ALL + "para el incentivo " + re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(id), 'Configuration'].values)[0]).group())
    else:
        columns = [col[0] for col in rows]
        types = [col[1] for col in rows]
        df = pd.DataFrame(columns=columns)
        for nombre, tipo in zip(columns, types):
            if tipo == 'int':
                df[nombre] = df[nombre].astype('Int64')
            elif tipo == 'varchar':
                df[nombre] = df[nombre].astype('string')
            elif tipo == 'decimal':
                df[nombre] = df[nombre].astype('float64')
            else:
                df[nombre] = df[nombre].astype('string')
        return df