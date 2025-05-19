# IMPORTANTE! 
Antes de ejecutar cualquiera de los dos scripts, asegurate de estar conectado a la VPN correspondiente, 
poseer las ligas de acceso en el archivo .Env y poseer el archivo 'cMappingTableActive.csv' en la carpeta utils

# MapActiveCheck 
Este proyecto debe ejecutarse primero (Y de manera Periodica, cada 1 Mes)
El script `MapActiveCheck.py` se encarga de verificar la existencia de Datos en la Base de Datos SQL de las tablas definidas en 'cMappingTableActive.csv' y 'cMappingTableOFF.csv'. 
Todas aquellas tablas existentes en 'cMappingTableActive.csv' que no tengan datos en SQL se almacenan en 'cMappingTableOFF.csv' y se elminan de 'cMappingTableActive.csv'.
Esto con el fin de evitar el uso de recursos innecesarios en tablas Vacias.


# AutoCuadre SQL vs ICM
`autoC.py` es un proyecto de Python que consulta la información de las tablas contenidas en el archivo 'cMappingTableActive.csv' en ICM como SQL.
La informacion se filtra por periodos de tiempo definidos en la estructura de las tablas de Incentivos y encuentra diferencias entre los datos de SQL y los de ICM.
Las diferencias se pueden categorizar en:
1. Inexistencias de Valores en ICM con respecto a SQL.
2. Registros Excedentes en ICM (Los cuales no existen en SQL).
3. Diferencias en el campo Value entre los registros que si existan en ambos sistemas.
4. Información general de las tablas de SQL e ICM.

# Puntos a tener en cuenta
* Para recibir los reportes SIN alteraciónes (es decir con los campos de procesamiento) debemos de ajustar el booleano [BOOL]['raw') en config.ini a True
* El script `autoC.py` debe ejecutarse después de `MapActiveCheck.py`.
* El script `autoC.py` no modifica datos, solo hace consultas y genera reportes.
* El script `MapActiveCheck.py` debe ejecutarse cada mes para mantener la un registro actualizado de Incentivos Vigentes.
* Durante la ejecución de `autoC.py`, puede mostrar el mensaje en consola "No se encontro informacion para el Incentivo X".
   Esto se refiere a que no se encontro informacion en las tablas de SQL durante los periodos definidos y no necesariamente indica que el Incentivo no contiene informacion.
* Se generarán archivos CSV en la carpeta 'Results' con información reelevante sobre las diferencias encontradas entre SQL e ICM.
* Los archivos generados son:
  * `EmpleadosFaltantesICM.csv`: Contiene la información de los registros que faltan en ICM.
  * `EmpleadosValueIncorrecto.csv`: Contiene la información de los registros con Value incorrecto en ICM.
  * `EmpleadosExcedentes.csv`: Contiene la información de los registros Excedentes en ICM.
  * `LogGeneralInfo.txt`: Contiene la información general de las categorias de diferencias encontradas.

## Dependencias

Para ejecutar este proyecto, necesitas tener instaladas las siguientes bibliotecas:

* `colorama`
* `openpyxl`
* `pandas`
* `pyodbc`
* `requests`
* `python-dotenv`


Puedes instalarlas usando `pip`:

En la terminal ejecuta: 

pip install -r requirements.txt