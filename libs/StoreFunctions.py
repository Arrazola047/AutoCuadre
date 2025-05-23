import shutil as sh
import pandas as pd
import re 
import os

cMap_path = os.path.join(os.path.dirname(__file__), '..', 'utils', 'cMappingTableActive.csv')
cMap = pd.read_csv(os.path.abspath(cMap_path), sep=';', dtype=str)

def LimpiaDirectorio(dir: str):
    #Creamos la carpeta Results si no existe
    os.makedirs(dir, exist_ok=True)
    for file in os.listdir(dir):
        file_path = os.path.join(dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
        elif os.path.isdir(file_path):
            sh.rmtree(file_path)

def CreaSubcarpetas(dir: str, id: str):
    # route = dir + r"\\" + re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(id), 'Configuration'].values)[0]).group() 
    route = dir + r"\\" + id
    os.makedirs(route, exist_ok=True)
    return route

def LimpiaDataFrame(df: pd.DataFrame):
    #Encontramos la posicion de la columna 'CONCAT'
    idx = df.columns.get_loc('CONCAT')
    # Seleccionar solo las columnas desde 'CONCAT' hacia la derecha
    df = df.iloc[:, idx:]
    return df

def AlmacenaResultados(df: pd.DataFrame, icm: pd.DataFrame, route: str, ids: str, raw: bool):
    #Encontramos el nombre del incentivo 
    # calc = re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group()

    #Si se solicita, limpiamos el DataFrame eliminando las columnas generadas en el cuadre
    if raw == False:
        # df.apply(LimpiaDataFrame, axis=1)
        # icm.apply(LimpiaDataFrame, axis=1)
        df = LimpiaDataFrame(df)
        icm = LimpiaDataFrame(icm)

    #Almacenamos los resultados en la carpeta de resultados
    df[(df['ExisteGRAL'] == False)].to_csv(route + r"\\" + ids + r' - EmpleadosFaltantesICM.csv', sep=',', index=False)
    df[(df['CoincideSOFT'] == False) & (df['ExisteGRAL'] == True)].to_csv(route + r"\\" + ids + r' - EmpleadosValueIncorrecto.csv', sep=',', index=False)
    icm[(icm['ExistePRD'] == False)].to_csv(route + r"\\" + ids + r' - EmpleadosExcedentesICM.csv', sep=',', index=False)