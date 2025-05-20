#### AQUI VAMOS A VER QUE TABLAS DE MappingTable CONTIENEN INFORMACION, SI ESTAN VACIAS SE GUARDAN EN TableOFF Y SE ELIMINAN DE MappingTable
from colorama import Fore, Style
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd 
import os

#Definicion de Ruta de Variable de Entorno
base = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(os.path.dirname(__file__), 'Env', '.Env')

#Variables de Entorno 
load_dotenv(dotenv_path)
sqlServer = os.environ.get('sqlServer')
dataBase = os.environ.get('dataBase')
uid = os.environ.get('uid')
pwd = os.environ.get('pwd')

## Definicion de los CSV Map Active y OFF 
map = pd.read_csv(r'utils/cMappingTableActive.csv', sep=';', dtype=str)
off = pd.read_csv(r'utils/cMappingTableOFF.csv', sep=';', dtype=str)

## Definicion de Variables para la creacion de Query's
    ##Definicion de id's de las tablas en ICM (Tanto activas como Inactivas)
URLid = map['ResultURLid'].tolist() + off['ResultURLid'].tolist()
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

    # Guardamos las tablas activas en el CSV de MappingTableActive
    for table in active_tables:
        if table not in map['ResultURLid'].tolist():
            map = pd.concat([map, pd.DataFrame({'ResultURLid': [table]})], ignore_index=True)
    # Guardamos las tablas inactivas en el CSV de MappingTableOFF
    for table in empty_tables:
        if table not in off['ResultURLid'].tolist():
            off = pd.concat([off, pd.DataFrame({'ResultURLid': [table]})], ignore_index=True)
finally:
    if cnxn:
        cnxn.dispose()
        print(Fore.YELLOW + "Conexión SQL Cerrada" + Style.RESET_ALL + "\n")

# Guardamos los resultados en los CSV correspondientes
map.to_csv('cMappingTableActive.csv', sep=';', index=False)
off.to_csv('cMappingTableOFF.csv', sep=';', index=False)