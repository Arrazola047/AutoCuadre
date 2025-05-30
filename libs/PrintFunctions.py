from colorama import Fore, Style
from libs.custom import *
import pandas as pd 
import re
import os

def ImprimeResumen(filtroPeriodo: list, df: pd.DataFrame, icm: pd.DataFrame, ids: str, route: str, cMap: pd.DataFrame, campoPeriodo: str, periodoInicial: int, periodoFinal: int):
    #Encontramos el nombre del incentivo
    calc = re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group()

    # Crear un archivo markdown y escribir información relevante
    md_filename = os.path.join(route, f"Resumen - {calc}.md")
    with open(md_filename, "w", encoding="utf-8") as md_file:
        #Escribimos los Faltantes
          #Encabezado
        md_file.write(f"# Faltantes para el Incentivo {calc}\n")
          #Body
        for i in filtroPeriodo:
            total = len(df[df[campoPeriodo] == i])
            existentes = df[df[campoPeriodo] == i]['CONCAT'].isin(icm['CONCAT']).sum()
            faltan = abs(existentes - total)
            md_file.write(f"- Periodo {i}: {existentes} de {total} registros, faltan {faltan} registros\n")
        total_existentes = len(df[df['ExisteGRAL'] == True])
        porcentaje = (total_existentes / len(df) * 100) if len(df) > 0 else 0
        md_file.write(f"**Existencia en ICM:** {total_existentes} / {len(df)} registros\n")
        md_file.write(f"**Porcentaje total de existencias:** {porcentaje:.2f}%\n")
        md_file.write(f"{'-'*50}\n")

        #Escribimos las Coincidencias
            #Encabezado
        md_file.write(f"\n# Coincidencias en Value para el Incentivo {calc}\n")
            #Body
        for i in filtroPeriodo:
            total = len(df[(df[campoPeriodo] == i) & (df['ExisteGRAL'] == True)])
            coincidencias = len(df[(df[campoPeriodo] == i) & (df['CoincideSOFT'] == True) & (df['ExisteGRAL'] == True)])
            faltan = abs(coincidencias - total)
            md_file.write(f"- Periodo {i}: {coincidencias} de {total} registros, faltan {faltan} registros\n")
        total_coincidencias = len(df[df['CoincideSOFT'] == True])
        porcentaje_coincidencias = (total_coincidencias / len(df) * 100) if len(df) > 0 else 0
        md_file.write(f"**Coincidencias en Value:** {total_coincidencias} / {len(df)} registros\n")
        md_file.write(f"**Porcentaje de coincidencias en Value:** {porcentaje_coincidencias:.2f}%\n")
        md_file.write(f"{'-'*50}\n")

        #Escribimos los Excedentes
            #Encabezado
        md_file.write(f"\n# Excedentes en ICM para el Incentivo {calc}\n")
            #Body
        for i in filtroPeriodo:
            total = len(icm[(icm['ExistePRD'] == False) & (icm[campoPeriodo] == i)])
            md_file.write(f"- Periodo {i}: {total} registros excedentes\n")
        total_excedentes = len(icm[icm['ExistePRD'] == False])
        md_file.write(f"**Total de Excedentes en ICM:** {total_excedentes} registros\n")
        md_file.write(f"{'-' * 50}\n")
        
        md_file.write(f"\n# Porcentaje general del cuadre en los periodos {periodoInicial} a {periodoFinal}: {len(df[(df['CoincideSOFT'] == True) & (df['ExisteGRAL'] == True)]) / len(df) * 100:.2f}%\n")  