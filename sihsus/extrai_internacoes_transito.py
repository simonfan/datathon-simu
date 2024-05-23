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

Os códigos de categoria utilizados para o filtro estão entre V01 e V89.

O dicionário de variáveis dos dados do SIHSUS pode ser visto no link abaixo:
https://pcdas.icict.fiocruz.br/conjunto-de-dados/sistema-de-informacoes-hospitalares-do-sus-sihsus/dicionario-de-variaveis/

A base de dados do Sistema de Informações Hospitalares do SUS - SIHSUS
é mantida e disponibilizada publicamente pela Plataforma de Ciência de Dados
aplicada à Saúde - PCDaS.
https://pcdas.icict.fiocruz.br/
"""

from functools import reduce
from multiprocessing import Pool
import pandas as pd
from zipfile import ZipFile

# Caminho para o zip com os dados do SIHSUS
ETLSIH_ZIP = 'ETLSIH.zip'
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
    'def_dias_perm',
    'DIAS_PERM',
    'QT_DIARIAS',
    'VAL_TOT',
    'res_codigo_adotado',
    'int_codigo_adotado',
    'res_SIGLA_UF',
    'int_SIGLA_UF'
]

# Classificadores de diagnóstico secundário.
# São nestas colunas que os códigos CID associados à acidentes de trânsito
# serão buscados.
COLS_CID = [
    'DIAG_SECUN',
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

COLS = COLS_ANALISE + COLS_CID

def extrair(de_arquivo):
    """Função responsável por extrair os dados de um arquivo do SIHSUS,
    filtrá-los de acordo com as colunas acima e passar o slice de volta
    ao caller.
    """

    with ZipFile(ETLSIH_ZIP) as z:
        with z.open(de_arquivo) as f:
            df = pd.read_csv(
                f,
                header=0,
                index_col=False,
                usecols=lambda col: col in COLS,
                dtype='object'
            )

    mask = reduce(
        lambda a,b: a|b,
        [c.str.match(r'V[0-8][0-9]', na=False) for n, c in df.items() if n in COLS_CID]
    )

    return df.loc[mask]

def main():

    with ZipFile(ETLSIH_ZIP) as z:
        namelist = z.namelist()

    with Pool() as pool:
        result = pd.concat(pool.map(extrair, namelist, 500), ignore_index=True)

    # TODO:
    # Seria interessante consolidar as diversas colunas de diagnóstico
    # secundário em uma só, contendo apenas o código relativo ao acidente
    # de trânsito.
    result.to_csv(CSV_DST, index=False, header=True)

    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
