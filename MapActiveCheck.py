#### AQUI VAMOS A VER QUE TABLAS DE MappingTable CONTIENEN INFORMACION, SI ESTAN VACIAS SE GUARDAN EN TableOFF Y SE ELIMINAN DE MappingTable
from colorama import Fore, Style
from dotenv import load_dotenv
import pandas as pd 
import pyodbc as db
import os

#Definicion de Ruta de Variable de Entorno
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

## Conexion a SQL 
cnxn = None
try: 
    conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER='+ sqlServer + ';'
        r'DATABASE='+ dataBase +';'
        r'UID=' + uid + ';'
        r'PWD=' + pwd + ';'
    )
    cnxn = db.connect(conn_str)
    cursor = cnxn.cursor()
    print(Fore.GREEN + "Connected to SQL Server" + Style.RESET_ALL)

    #Por cada id en URLid, se ejecuta la consulta para contar el número de filas en la tabla correspondiente
    for id in URLid:
        #Query para la estructura de la tabla table = '\"_Result119\"'
        cursor.execute(f"SELECT COUNT(*), '{id}' AS ResultURLid FROM \"_Result{id}\"")
        rows = cursor.fetchall()
        if len(rows) == 0:
            print(Fore.RED + "No data found" + Style.RESET_ALL)
        elif rows[0][0] == 0:
            # Encontrar el registro en map con el ResultURLid correspondiente
            mask = map['ResultURLid'] == str(rows[0][1]).strip("'")
            ctrlX = map[mask]
            # Agregarlo a off
            off = pd.concat([off, ctrlX], ignore_index=True)
            # Eliminarlo de map
            map = map[~mask]
            #Actualizar el RowCount en off
            off.loc[off['ResultURLid'] == str(rows[0][1]).strip("'"), 'RowCount'] = rows[0][0]
        else:
            # Actualizar el RowCount en map
            map.loc[map['ResultURLid'] == str(rows[0][1]).strip("'"), 'RowCount'] = rows[0][0]

## Cerramos Conexion a SQL tanto si se ha ejecutado correctamente como si no
except db.Error as ex:
    sqlstate = ex.args[0]
    print(Fore.RED + "Error in SQL Connection " + Style.RESET_ALL + str(sqlstate))
finally:
    if cnxn:
        cursor.close()
        cnxn.close()

# Guardamos los resultados en los CSV correspondientes
map.to_csv('cMappingTableActive.csv', sep=';', index=False)
off.to_csv('cMappingTableOFF.csv', sep=';', index=False)



# def SQLEmpty(cnxn: any, URLid: list, where: str):
#     #Query Data
#     tables = ','.join([f"'_Result{i}'" for i in URLid])
#     query = text(f"""SELECT 
# 	                OBJECT_NAME(p.object_id) AS TableName,
# 	                SUM(p.row_count) AS TotalRows
#                     FROM sys.dm_db_partition_stats p
#                     WHERE OBJECT_NAME(p.object_id) IN ({tables})
#                     GROUP BY OBJECT_NAME(p.object_id)""")
#     print(query)
#     return URLid