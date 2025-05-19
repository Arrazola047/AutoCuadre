from libs.custom import *
import pandas as pd

def Existencias(prd: pd.DataFrame, icm: pd.DataFrame):
    #Validaciones de Existencias
    prd['ExisteGRAL'] = prd['CONCAT'].isin(icm['CONCAT']).astype(bool)
    prd['ValorICM'] = prd['CONCAT'].map(icm.set_index('CONCAT')['Value']).fillna("0")

    #Validaciones de Coincidencias
    prd['CoincidenciaHARD'] = prd['Value'].astype(float) == prd['ValorICM'].astype(float) 
    prd['CoincideSOFT'] = prd['ValorICM'].astype(float).sub(prd['Value'].astype(float)).abs().lt(0.9)
    
    #Validacion de Excedentes
    icm['ExistePRD'] = icm['CONCAT'].isin(prd['CONCAT']).astype(bool)

    return prd, icm

def Diffs(prd: pd.DataFrame):
    #Comparativa de Diferencias en Value
    prd['Diferencia'] = prd.apply(
        lambda x: (
            abs(float(x['Value']) - float(x['ValorICM']))
        ), axis = 1
    )
    return prd

def Pctg(prd: pd.DataFrame):
    #Identificacion de Tipo de Diferencia
    prd['Tipo'] = prd.apply(identificar_multipo, axis=1)
    prd['Pctg Diff PRD'] = (pd.to_numeric(prd['Diferencia'], errors='coerce') / pd.to_numeric(prd['Value'], errors='coerce')).abs() * 100
    prd['Pctg Diff PRD'] = prd['Pctg Diff PRD'].fillna(0)
    prd['Pctg Diff ICM'] = (pd.to_numeric(prd['Diferencia'], errors='coerce') / pd.to_numeric(prd['ValorICM'], errors='coerce')).abs() * 100
    prd['Pctg Diff ICM'] = prd['Pctg Diff ICM'].fillna(0)

    return prd

def HardRound(prd: pd.DataFrame):
    #Redondeo Estricto
    prd['HardRoundPRD'] = prd['Pctg Diff PRD'].apply(ajustar_a_0_o_5)
    prd['HardRoundPRD'] = prd['HardRoundPRD'].astype(str).str.replace('.0', '', regex=False) + '%'
    prd['HardRoundICM'] = prd['Pctg Diff ICM'].apply(ajustar_a_0_o_5)
    prd['HardRoundICM'] = prd['HardRoundICM'].astype(str).str.replace('.0', '', regex=False) + '%'

    return prd