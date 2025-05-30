from datetime import datetime, timedelta
from colorama import Fore, Style
import configparser
import pandas as pd
import subprocess
import re 
import os

base = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(base, '..', 'config', 'config.ini'))

def identificar_multipo(row): 
    try: 
        valuePRD = float(row['Value'])
        valueICM = float(row['ValorICM'])
    except ValueError:
        return "Error de conversion Numerica"
    
    tolerancia = 0.011

    if pd.isna(valueICM) or pd.isna(valuePRD) or valueICM == 0 or valuePRD == 0:
        return "Valor PRD o ICM es 0 o NULL"

    if abs((valueICM / valuePRD) - 2) <= tolerancia:
        return "ValorICM ≈ 2x Value (Posible Duplicado)"
    elif abs((valueICM / valuePRD) - 3) <= tolerancia:
        return "ValorICM ≈ 3x Value (Posible Triplicado)"
    elif abs((valueICM / valuePRD) - 4) <= tolerancia:
        return "ValorICM ≈ 4x Value (Posible Cuadruplicado)"
    elif abs((valueICM / valuePRD) - 5) <= tolerancia:
        return "ValorICM ≈ 5x Value"
    else : 
        return "Otro" 
    
def ajustar_a_0_o_5(valor):
    try:
        valor = float(valor)  # Convertir a float
        ultimo_digito = valor % 10
        if ultimo_digito < 5:
            return round(valor - ultimo_digito + (5 if ultimo_digito != 0 else 0))  # Redondear hacia el múltiplo más cercano de 5
        else:
            return round(valor + (10 - ultimo_digito))  # Redondear hacia el múltiplo más cercano de 5
    except ValueError:
        return valor  # Devolver el valor original si no es numérico

def colorPCTG(x):
    if x < 50: 
        color = Fore.RED + Style.BRIGHT
    elif x < 80:
        color = Fore.YELLOW + Style.BRIGHT
    elif x < 100:
        color = Fore.LIGHTYELLOW_EX + Style.BRIGHT
    else:
        color = Fore.GREEN + Style.BRIGHT
    return color

def validaActiveCheck(lastDate: str, Modelo: str, tolerancia: int):
    if datetime.now() - datetime.strptime(lastDate, "%d/%m/%Y") > timedelta(days=tolerancia):
        print(Fore.RED + "ALERTA!!! - La ultima Verificacion de Tablas activas fue hace mas de 15 dias" + Style.RESET_ALL)
        print(Fore.YELLOW + "Ejecutando Verificacion de Tablas..." + Style.RESET_ALL + "\n")
        subprocess.run(['python', 'Scripts/MapActiveCheck.py'], check=True)
        print(Fore.GREEN + "Verificación ejecutada correctamente" + Style.RESET_ALL)
        config[Modelo]['lastCheck'] = datetime.now().strftime("%d/%m/%Y")

##NO USADO
def ObtenerPeriodoREGEX(id: pd.DataFrame): 
    campoPeriodo = next(
        (col for col in id.columns if re.match(r'^\d{4},.+?\d{2}$', str(id.iloc[0][col]))),
        None
    )    
    return campoPeriodo