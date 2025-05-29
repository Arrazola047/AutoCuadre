from libs.DataProcessFunctions import *
from sqlalchemy import create_engine
from colorama import Fore, Style
from libs.PrintFunctions import *
from libs.StoreFunctions import *
from libs.SQLFunctions import *
from libs.ICMFunctions import *
from dotenv import load_dotenv
from libs.custom import *
import configparser
import pandas as pd
import os

##################################### Definicion de Variables #####################################

### Carga de Variables de Configuracion ###
base = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(base, 'config', 'config.ini'))
ModelList = [m.strip() for m in config['MODELOS']['List'].split(',')]

# #Consulta de Modelo 
# print(f"{Fore.BLUE}Que modelo quieres consultar?{Style.RESET_ALL}")
# for i in ModelList:
#     print(i)

# while True:
#     Modelo = input(f"{Fore.YELLOW}Ingresa el nombre del modelo:{Style.RESET_ALL} ").strip()
#     if Modelo in ModelList:
#         break
#     else:
#         print(f"{Fore.RED}Modelo no válido. Por favor, ingresa un modelo de la lista.{Style.RESET_ALL}")

# # Limpiar la terminal antes de continuar
# os.system('cls' if os.name == 'nt' else 'clear')

Modelo = 'HNKQnc'

#Utilidades de Modelos
MapArchive = config[Modelo]['MapArchive']
EnvDir = config[Modelo]['Env_dir']

# Definicion de Rutas
plurals = os.path.join(base, 'utils', 'plurals.json')
cMap = pd.read_csv(os.path.join(base, 'utils', MapArchive), sep=';', dtype=str)
dotenv_path = os.path.join(base, EnvDir, '.Env') 
load_dotenv(dotenv_path)
output_dir = os.path.join(base, '..', 'Resultados')

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
lastDate = config[Modelo]['ultimaComprobacion']
queryYear = config[Modelo]['queryYear']
periodoType = config[Modelo]['periodoType']
periodoInicial = config[Modelo]['periodoInicial']
periodoFinal = config[Modelo]['periodoFinal']
URLid = cMap['ResultURLid'].tolist()
qEmpty = []

#Variables de Query
filtroPeriodo = [f"{queryYear}, {json.load(open(plurals)).get(periodoType, periodoType)} {str(i).zfill(2)}" for i in range(int(periodoInicial), int(periodoFinal) + 1)]
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
    if cnxn: 
        print(Fore.GREEN + "Conexión SQL Establecida..." + Style.RESET_ALL+ "\n")

    #Obtener las tablas con datos
    URLid = SQLEmpty(cnxn,URLid, where, cMap)
    
    print(Fore.GREEN + "\nObteniendo Registros..." + Style.RESET_ALL)
    # Obtenemos los datos de las tablas
    for ids in URLid:
        globals()[ids] = pd.DataFrame()
        globals()[ids] = SQLSearch(cnxn, ids, where, order, globals()[ids])
        print(f"{Fore.BLUE}{re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group()} - {Style.RESET_ALL}{len(globals()[ids])} Registros obtenidos")
finally:
    if cnxn:
        cnxn.dispose()
        print("\n" + Fore.YELLOW + "Conexión SQL Cerrada" + Style.RESET_ALL + "\n")
        print(f"{Fore.LIGHTBLACK_EX}{"#" * 50}{Style.RESET_ALL}\n")

######################### Peticion al API #########################
print(Fore.YELLOW + "Conectando a la API de ICM..." + Style.RESET_ALL)
for id in URLid:
    #Generamos el nombre de los DF de ICM
    icm = id + 'ICM'

    #Generamos el DF
    globals()[icm] = pd.DataFrame()

    #Generamos el payload para la peticion
    data = getPayload(id, where, order)
    # Peticion al API y Manejo de Respuesta
    response =  getResponse(apiurl, header, data, id, cMap)
    
    # Serializacion de la respuesta JSON a DataFrame
    jResponse = pd.json_normalize(response.json())

    #Construccion del DataFrame
    if jResponse.empty or response.status_code != 200:
        print(Fore.RED + "Error al ejecutar el Query" + Style.RESET_ALL)
    elif len(jResponse.data[0]) == 0:
        print(f"{Fore.RED}ALERTA!!! - No se encontraron datos en ICM para los periodos especificados {Style.RESET_ALL}")
        qEmpty.append(id)
        ### AGREGAR LOGICA PARA ALTERAR UNA VARIABLE EN CONFIG PARA EN OTRO CODIGO AUTOMATIZAR LA SINCRONIZACION DE TABLAS VACIAS A LA API 
    else: 
        globals()[icm] = construyeDataFrame(jResponse, globals()[icm])

    #Eliminamos las tablas vacias (Resultantes del query a ICM) para no procesarlas
    URLid = [x for x in URLid if x not in qEmpty]
print(f"{Fore.GREEN}Peticiones a ICM Finalizadas{Style.RESET_ALL}"+ "\n")
print(f"{Fore.LIGHTBLACK_EX}{"#" * 50}{Style.RESET_ALL}\n")

########################## Procesamiento de DataFrames #########################
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
# ########################## Impresión de Resultados #########################
# # id = URLid[0]
# campoPeriodo = ObtenerPeriodoREGEX(globals()[URLid[0]]) 

# #Impresion de Faltantes
# # for id in URLid:
# #     icm = id + 'ICM'
# #     ImprimeFaltantes(filtroPeriodo, campoPeriodo, globals()[id], globals()[icm], id)
# #     ImprimeCoincidencias(filtroPeriodo, campoPeriodo, globals()[id], id)
# #     ImprimeExcedentes(filtroPeriodo, campoPeriodo, globals()[icm], id)
# #     ImprimeGeneral(globals()[id], id)

########################## Almacenado de Resultados #########################
# NOTA IMPORTANTE!!!! 
# Los resultados anteriormente almacenados en la carpeta Results seran sobreescritos por los resultados de la ejecucion actual
# Se recomienda cambiar el nombre de la carpeta Results a Results_YYYYMMDD para evitar sobreescritura o almacenar los resultados en una carpeta diferente

#Limpiamos la carpeta de resultados
LimpiaDirectorio(output_dir)
#Almacenamos los resultados 
for id in URLid:
    #Creamos subcarpetas
    route = CreaSubcarpetas(output_dir, id, cMap)
    #Almacenamos los resultados
    AlmacenaResultados(globals()[id], globals()[id + 'ICM'], route, id, raw, cMap)
print(Fore.GREEN + "Resultados almacenados en ../Results" + Style.RESET_ALL)