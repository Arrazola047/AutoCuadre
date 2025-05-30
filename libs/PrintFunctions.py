from colorama import Fore, Style
from libs.custom import *
import pandas as pd 
import os



def ImprimeCoincidencias(filtroPeriodo: list, campoPeriodo: str, df: pd.DataFrame, ids: str, cMap: pd.DataFrame):
    print(Fore.GREEN + "Coincidencias " + Style.RESET_ALL + "VALUE en Registros Existentes Para el Incentivo " + re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group() + "\n")

    for i in filtroPeriodo:
        print(f"Del periodo {i} tenemos {len(df[(df[campoPeriodo] == i) & (df['CoincideSOFT'] == True) & (df['ExisteGRAL'] == True)])} registros de {len(df[(df[campoPeriodo] == i) & (df['ExisteGRAL'] == True)])}, faltan {abs(len(df[(df[campoPeriodo] == i) & (df['CoincideSOFT'] == True) & (df['ExisteGRAL'] == True)]) - len(df[(df[campoPeriodo] == i) & (df['ExisteGRAL'] == True)]))} registros")
    print(f"{"-"*50}\n")
    
    if len(df[df['CoincideSOFT'] == False]) > 0:
        print(f"Del total de registros que existen tanto en PRD como en ICM tenemos una coincidencia de {len(df[(df['CoincideSOFT'] == True) & (df['ExisteGRAL'] == True)])} / {len(df[df['ExisteGRAL'] == True])} registros, faltan {len(df[(df['CoincideSOFT'] == False) & (df['ExisteGRAL'] == True)])} Registros por revisar")
    else:
        print(f"Del total de registros que existen tanto en PRD como en ICM, tenemos una coincidencia de {len(df[df['CoincideSOFT'] == True])} registros")
    print(f"Porcentaje de Coincidencias en Value del incentivo " + re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group() + " con respecto a los registros existentes: " + Fore.BLUE + f"{len(df[df['CoincideSOFT'] == True]) / len(df) * 100:.2f}%" + Style.RESET_ALL)
    print(f"\n{"#"*50}\n")

def ImprimeFaltantes(filtroPeriodo: list, campoPeriodo: str, df: pd.DataFrame, icm: pd.DataFrame, ids: str, cMap: pd.DataFrame):
    print(Fore.RED + "FALTANTES " + Style.RESET_ALL + "Para el Incentivo " + re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group() + "\n")

    for i in filtroPeriodo:
        print(f"Del periodo {i} tenemos {df[df[campoPeriodo] == i]['CONCAT'].isin(icm['CONCAT']).sum()} registros de {len(df[df[campoPeriodo] == i])}, faltan {abs(df[df[campoPeriodo] == i]['CONCAT'].isin(icm['CONCAT']).sum() - len(df[df[campoPeriodo] == i]))} registros")
    print(f"{"-"*50}\n")

    print(f"En el modelo de ICM, tenemos una Existencia de {len(df[df['ExisteGRAL'] == True])} / {len(df)} registros con respecto a PRD")
    print(f"Porcentaje total de Existencias del incentivo " + re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group() + "en los periodos a trabajar: " + Fore.BLUE + f"{len(df[df['ExisteGRAL'] == True]) / len(df) * 100:.2f}%" +  Style.RESET_ALL)
    print(f"\n{"#"*50}\n")

def ImprimeExcedentes(filtroPeriodo: list, campoPeriodo: str, icm: pd.DataFrame, ids: str, cMap: pd.DataFrame):
    print(Fore.YELLOW + "Excedentes " + Style.RESET_ALL + "de ICM Para el Incentivo " + re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group() +  "\n")

    for i in filtroPeriodo:
        print(f"Del periodo {i} tenemos {len(icm[(icm['ExistePRD'] == False) & (icm[campoPeriodo] == i)])} registros excedentes")
    print(f"\n{"#"*50}\n")

def ImprimeGeneral(df: pd.DataFrame, ids: str, cMap: pd.DataFrame):
    color = Fore.WHITE + Style.BRIGHT
    reset = Style.RESET_ALL
    
    operacionA = (len(df[(df['CoincideSOFT'] == True) & (df['ExisteGRAL'] == True)]) / len(df) * 100)
    print(Fore.LIGHTMAGENTA_EX + "Estatus general del Incentivo " + reset + re.match(r'^[^ ]*', (cMap.loc[cMap['ResultURLid'] == str(ids), 'Configuration'].values)[0]).group() +  "\n")
    color = colorPCTG(operacionA)
    print("Porcentaje del calculo correspondiente con los periodos a trabajar: " + color + f"{operacionA:.2f}%" + reset)
    print(f"\n{"#"*50}\n")