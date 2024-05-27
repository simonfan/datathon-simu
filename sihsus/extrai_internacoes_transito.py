"""
Este script é utilizado para extrair da base do SIHSUS as informações
de internacões causadas por acidentes de trânsito e consolidá-las
em um único arquivo csv.

Como a base está segmentada em diversos arquivos, é necessário carregar
cada um deles em um DataFrame, aplicar os filtros desejados e, por fim,
contanenar o resultado em um único produto final. Este processo é agilizado
carregando os arquivos em paralelo com multiprocessing.Pool.

O recorte das informações do SIHSUS é feito em dois eixos. Primeiro, carregando
apenas as colunas que serão utilizadas na análise posterior dos dados. Depois,
filtrando as linhas de acordo com o código do diagnóstico secundário.

A classificação utilizada pelo SUS, CID-10, pode ser encontrada no link abaixo:
http://www2.datasus.gov.br/cid10/V2008/cid10.htm

O dicionário de variáveis dos dados do SIHSUS pode ser visto no link abaixo:
https://pcdas.icict.fiocruz.br/conjunto-de-dados/sistema-de-informacoes-hospitalares-do-sus-sihsus/dicionario-de-variaveis/

A base de dados do Sistema de Informações Hospitalares do SUS - SIHSUS
é mantida e disponibilizada publicamente pela Plataforma de Ciência de Dados
aplicada à Saúde - PCDaS.
https://pcdas.icict.fiocruz.br/
"""

from multiprocessing import Pool
import pandas as pd
import re
from zipfile import ZipFile

# Caminho para o zip com os dados do SIHSUS
ETLSIH_ZIP = '../../etlsih/ETLSIH.zip'
# Nome do arquivo consolidado, que será salvo localmente
CSV_DST = 'etlsih_transito.csv'

# Colunas interessantes para análise dos dados
COLS_ANALISE = [
    'def_sexo',
    'def_instru',
    'def_raca_cor',
    'def_idade_anos',
    'def_cbo',
    'def_cnae',
    'def_morte',
    'dt_inter',
    'VAL_TOT',
    'res_codigo_adotado',
    'def_micro_res',
    'def_meso_res',
    'res_SIGLA_UF',
    'int_codigo_adotado',
    'def_micro_int',
    'def_meso_int',
    'int_SIGLA_UF',
]

# Classificadores de diagnóstico secundário.
# São nestas colunas que os códigos CID associados à acidentes de trânsito
# serão buscados.
COLS_CID = [
    'DIAG_PRINC',
    'DIAG_SECUN',
    'CID_ASSO',
    'CID_MORTE',
    'DIAGSEC1',
    'DIAGSEC2',
    'DIAGSEC3',
    'DIAGSEC4',
    'DIAGSEC5',
    'DIAGSEC6',
    'DIAGSEC7',
    'DIAGSEC8',
    'DIAGSEC9',
]

COL_CAR_INT = ['def_car_int']

COLS = COLS_ANALISE + COLS_CID + COL_CAR_INT

def busca_regex(elemento, regex):
    """
    Retorna True se regex puder ser encontrada no elemento.
    `regex` deve ser uma expressão compilada com re.compile().
    """
    if regex.search(elemento) is None:
        return False
    else:
        return True

def extrair(de_arquivo):
    """
    Função responsável por extrair os dados de um arquivo do SIHSUS,
    filtrá-los de acordo com as colunas acima e passar o slice de volta
    ao caller.
    """

    regex = re.compile(
        r"""^V(
             (?:[1-7][0-9][5679])  # Diversos casos de acidentes de trânsito
            |(?:0[0-8][19])        # Pedestre em colisão de trânsito
            |(?:09[23])            # Pedestre em outros acidentes de trânsito
            |(?:[1-7]94)           # Colisão com veículos não especificados
            |(?:[12][0-8]4)        # Especificidade ciclista e motociclista
            |(?:87[0-9])           # Pessoa traumatizada em acidente de trânsito
            |(?:8[3-6][0-3])       # Veículos especiais em acidente de trânsito
            |(?:89[23])            # Acidente com veículo não especificado
        )""",
    re.X)

    with ZipFile(ETLSIH_ZIP) as z:
        with z.open(de_arquivo) as f:
            df = pd.read_csv(
                f,
                header=0,
                index_col=False,
                usecols=lambda col: col in COLS,
                dtype='object'
            )

    cols_cid = [c for c in df.columns if c in COLS_CID]

    mask_cid = df[cols_cid].map(busca_regex, na_action='ignore', regex=regex)
    mask_car_int = df[COL_CAR_INT[0]].str.contains('trânsito', na=False, regex=False)

    mask = mask_cid.any(axis='columns') | mask_car_int

    return df.loc[mask]

def main():

    with ZipFile(ETLSIH_ZIP) as z:
        namelist = z.namelist()

    with Pool() as pool:
        result = pd.concat(pool.map(extrair, namelist, 100), ignore_index=True)

    result.to_csv(CSV_DST, index=False, header=True)

    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
