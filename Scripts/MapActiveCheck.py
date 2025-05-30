#### AQUI VAMOS A VER QUE TABLAS DE MappingTable CONTIENEN INFORMACION, SI ESTAN VACIAS SE GUARDAN EN TableOFF Y SE ELIMINAN DE MappingTable
from colorama import Fore, Style
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import configparser
import pandas as pd 
import os

#Definicion de Ruta de Variable de Entorno
base = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(base, '..', 'config', 'config.ini'))
dotenv_path = os.path.join(base, '..', 'Env', '.Env')
Modelo = config['SELECCION']['model']
MapArchive = config[Modelo]['MapArchive']

#Variables de Entorno 
load_dotenv(dotenv_path)
sqlServer = os.environ.get(config[Modelo]['EnvStr'] + 'sqlServer')
dataBase = os.environ.get(config[Modelo]['EnvStr'] +'dataBase')
uid = os.environ.get(config[Modelo]['EnvStr'] + 'uid')
pwd = os.environ.get(config[Modelo]['EnvStr'] + 'pwd')

## Definicion de los CSV Map Active y OFF 
cMap = pd.read_csv(os.path.join(base, '..', 'utils', MapArchive) + '.csv', sep=';', dtype=str)

## Definicion de Variables para la creacion de Query's
    ##Definicion de id's de las tablas en ICM (Tanto activas como Inactivas)
URLid = cMap['ResultURLid'].tolist()
tables = ','.join([f"'_Result{i}'" for i in URLid])
conn_str = (f"mssql+pyodbc://{uid}:{pwd}@{sqlServer}/{dataBase}?driver=ODBC+Driver+17+for+SQL+Server")
cnxn = None
try: 
    cnxn = create_engine(conn_str)
    print(Fore.GREEN + "Connected to SQL Server" + Style.RESET_ALL)

    # Consultamos los metadatos de las tablas SQL
    query = text(f"""SELECT 
 	                OBJECT_NAME(p.object_id) AS TableName,
 	                SUM(p.row_count) AS TotalRows
                    FROM sys.dm_db_partition_stats p
                    WHERE OBJECT_NAME(p.object_id) IN ({tables})
                    GROUP BY OBJECT_NAME(p.object_id)""")
    df = pd.read_sql(query, cnxn)

    # Filtramos las tablas que tienen 0 filas
    empty_tables = df[df['TotalRows'] == 0]['TableName'].tolist()
    # Filtramos las tablas que tienen filas
    active_tables = df[df['TotalRows'] > 0]['TableName'].tolist()

    # Actualizamos el status de activo en el archivo de mapeo de calculos finales
    for table in active_tables:
        cMap.loc[cMap['ResultURLid'] == int(table.replace('_Result', '')), 'Active'] = 1
    for table in empty_tables:
        cMap.loc[cMap['ResultURLid'] == int(table.replace('_Result', '')), 'Active'] = 0
finally:
    if cnxn:
        cnxn.dispose()
        print(Fore.YELLOW + "Conexión SQL Cerrada" + Style.RESET_ALL + "\n")

# Guardamos los resultados en los CSV correspondientes
cMap.to_csv(os.path.join(base, '..', 'utils', MapArchive) + '.csv', sep=';', index=False)