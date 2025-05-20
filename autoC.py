from libs.DataProcessFunctions import *
from libs.PrintFunctions import *
from libs.StoreFunctions import *
from libs.SQLFunctions import *
from libs.ICMFunctions import *
from colorama import Fore, Style
from dotenv import load_dotenv
from sqlalchemy import create_engine
from libs.custom import *
import configparser
import pandas as pd 

##################################### Definicion de Variables #####################################
# Definicion de Rutas
base = os.path.dirname(os.path.abspath(__file__))
map = pd.read_csv(os.path.join(base, 'utils', 'cMappingTableActive.csv'), sep=';', dtype=str)
dotenv_path = os.path.join(base, 'Env', '.Env')
# output_dir = r'..\Resultados'
output_dir = os.path.join(base, '..', 'Resultados')

### Carga de Varialbes de Entorno ###
dotenv_path = os.path.join(base, 'Env', '.Env')
load_dotenv(dotenv_path)

### Carga de Variables de Configuracion ###
config = configparser.ConfigParser()
config.read(os.path.join(base, 'config', 'config.ini'))

#Variables de Entorno SQL
sqlServer = os.environ.get('sqlServer')
dataBase = os.environ.get('dataBase')
uid = os.environ.get('uid')
pwd = os.environ.get('pwd')

#Variables de Entorno ICM
bearerToken = os.environ.get('bearerToken')
model = os.environ.get('model')
apiurl = os.environ.get('apiurl')

## Variables de Configuracion
raw = config['BOOL'].getboolean('raw')
lastDate = config['DATE']['ultimaComprobacion']
queryYear = '2024'
periodoType = 'Weeks'
periodoInicial = '10'
periodoFinal = '30'
URLid = map['ResultURLid'].tolist()
# URLid = ['119']
qEmpty = []
limit = 900

#Variables de Query
filtroPeriodo = [f"{queryYear}, {periodoType[:-1 if periodoType.endswith('s') else periodoType]} {str(i).zfill(2)}" for i in range(int(periodoInicial), int(periodoFinal) + 1)]
where = f'WHERE \"{periodoType}\" IN ({str(filtroPeriodo).strip("[]")})'
order = 'ORDER BY \"PayeeID_\"'

# #Variables de conexion a SQL
conn_str = (f"mssql+pyodbc://{uid}:{pwd}@{sqlServer}/{dataBase}?driver=ODBC+Driver+17+for+SQL+Server")
cnxn = None

## Variables de Conexion a ICM
header = {
    "Authorization": f"Bearer {bearerToken}",
    "Content-Type": "application/json",
    "Model": model
}

######################### Conexion a SQL #########################
try:
    cnxn = create_engine(conn_str)
    print(Fore.GREEN + "Connected to SQL Server" + Style.RESET_ALL)

    #Eliminar las tablas vacias de la lista
    URLid = SQLEmpty(cnxn,URLid, where)

    # Obtener el header y datos de la tabla
    for ids in URLid:
        globals()[ids] = pd.DataFrame() 
        globals()[ids] = SQLSearch(cnxn, ids, limit, where, order, globals()[ids])
finally:
    if cnxn:
        cnxn.dispose()
        print(Fore.YELLOW + "Conexión SQL Cerrada" + Style.RESET_ALL + "\n")

######################### Peticion al API #########################
print(Fore.YELLOW + "Conectando a la API de ICM..." + Style.RESET_ALL)
for id in URLid:
    icm = id + 'ICM'
    globals()[icm] = pd.DataFrame()
    data = getPayload(id, where, order, limit)
    # Peticion al API y Manejo de Respuesta
    response =  getResponse(apiurl, header, data, id)
    
    # Serializacion de la respuesta JSON a DataFrame
    jResponse = pd.json_normalize(response.json())

    #Construccion del DataFrame
    if jResponse.empty or response.status_code != 200:
        print(Fore.RED + "Error al ejecutar el Query" + Style.RESET_ALL)
    elif len(jResponse.data[0]) == 0:
        print(Fore.RED + "ALERTA!!! - No se encontraron datos en ICM para los periodos especificados" + Style.RESET_ALL)
        qEmpty.append(id)
        ### AGREGAR LOGICA PARA ALTERAR UNA VARIABLE EN CONFIG PARA EN OTRO CODIGO AUTOMATIZAR LA SINCRONIZACION DE TABLAS VACIAS A LA API 
    else: 
        globals()[icm] = construyeDataFrame(jResponse, globals()[icm])

    #Eliminamos las tablas vacias (Resultantes del query a ICM)
    URLid = [x for x in URLid if x not in qEmpty]

########################## Procesamiento de DataFrames #########################
print("\n" + Fore.YELLOW + "Procesando DataFrames..." + Style.RESET_ALL)
for id in URLid:
    icm = id + 'ICM'
    #Definicion de Existencias y Variables
    globals()[id], globals()[icm] = Existencias(globals()[id], globals()[icm])

    #Diferencia entre registros Coincidentes
    globals()[id] = Diffs(globals()[id])

    #Ejecucion de porcentajes de diferencia
    globals()[id] = Pctg(globals()[id])

    # Ejecucion de redondeo estricto
    globals()[id] = HardRound(globals()[id])
print(Fore.GREEN + "DataFrames procesados" + Style.RESET_ALL)
########################## Impresión de Resultados #########################
# id = URLid[0]
campoPeriodo = ObtenerPeriodoREGEX(globals()[URLid[0]]) 

#Impresion de Faltantes
# for id in URLid:
#     icm = id + 'ICM'
#     ImprimeFaltantes(filtroPeriodo, campoPeriodo, globals()[id], globals()[icm], id)
#     ImprimeCoincidencias(filtroPeriodo, campoPeriodo, globals()[id], id)
#     ImprimeExcedentes(filtroPeriodo, campoPeriodo, globals()[icm], id)
#     ImprimeGeneral(globals()[id], id)
    

########################## Almacenado de Resultados #########################
# NOTA IMPORTANTE!!!! 
# Los resultados anteriormente almacenados en la carpeta Results seran sobreescritos por los resultados de la ejecucion actual
# Se recomienda cambiar el nombre de la carpeta Results a Results_YYYYMMDD para evitar sobreescritura o almacenar los resultados en una carpeta diferente

#Limpiamos la carpeta de resultados
LimpiaDirectorio(output_dir)
#Almacenamos los resultados 
for id in URLid:
    #Creamos subcarpetas
    route = CreaSubcarpetas(output_dir, id)

    #Almacenamos los resultados
    AlmacenaResultados(globals()[id], globals()[id + 'ICM'], route, id, raw)