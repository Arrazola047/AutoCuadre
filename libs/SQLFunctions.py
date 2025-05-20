from colorama import Fore, Style
import pandas as pd
import os

sqlTypeMap = {
    'int': 'Int64',
    'bigint': 'Int64',
    'smallint': 'Int64',
    'tinyint': 'Int64',
    'bit': 'boolean',
    'float': 'float64',
    'real': 'float64',
    'decimal': 'float64',
    'numeric': 'float64',
    'money': 'float64',
    'smallmoney': 'float64',
    'date': 'datetime64[ns]',
    'datetime': 'datetime64[ns]',
    'datetime2': 'datetime64[ns]',
    'smalldatetime': 'datetime64[ns]',
    'char': 'string',
    'varchar': 'string',
    'text': 'string',
    'nchar': 'string',
    'nvarchar': 'string',
    'ntext': 'string'
}
# cMap = pd.read_csv(r'../utils/cMappingTableActive.csv', sep=';', dtype=str)
cMap_path = os.path.join(os.path.dirname(__file__), '..', 'utils', 'cMappingTableActive.csv')
cMap = pd.read_csv(os.path.abspath(cMap_path), sep=';', dtype=str)

def SQLSearch(cnxn: any, ids:str, limit:int, where:str, order:str, df: pd.DataFrame):
    #Query Data
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
    # Eliminamos las columnas que no tienen datos
    df = df.dropna(axis=1, how='all')
    # Hacemos una lista con las columnas que tienen datos para posteriormente solo buscar en esas tablas 
    URLid = df.columns.tolist()
    return URLid