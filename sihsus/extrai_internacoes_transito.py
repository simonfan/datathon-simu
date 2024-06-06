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

from functools import reduce
from multiprocessing import Pool
import pandas as pd
from zipfile import ZipFile

# Caminho para o zip com os dados do SIHSUS
ETLSIH_ZIP = '../../etlsih/ETLSIH.zip'
# Nome do arquivo consolidado, que será salvo localmente
CSV_DST = 'etlsih_transito.csv'

# Colunas interessantes para análise dos dados
COLS_ANALISE = [
    'dt_inter',
    'def_sexo',
    'def_instru',
    'def_raca_cor',
    'def_idade_anos',
    'def_cbo',
    'def_cnae',
    'MORTE'
    'VAL_TOT',
    'VAL_UTI',
    'res_codigo_adotado',
    'res_MUNNOME'
    'res_SIGLA_UF',
    'int_codigo_adotado',
    'int_MUNNOME',
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

# A coluna "Caráter da Internação" pode trazer a marcação
# "Outros tipo de acidente de trânsito" nos registros mais antigos,
# o que corresponde ao código 05.
COL_CAR_INT = ['CAR_INT']

COLS = COLS_ANALISE + COLS_CID + COL_CAR_INT

def extrair(de_arquivo):
    """
    Função responsável por extrair os dados de um arquivo do SIHSUS,
    filtrá-los de acordo com as colunas acima e passar o slice de volta
    ao caller.

    Aplica dois métodos para filtrar os registros de acidentes de trânsito:
    por CID e pela coluna CAR_INT.

    Também, adiciona a coluna `ano_mes_registro` à tabela final, para manter
    a marcação de qual foi ano e mês em que a AIH apareceu na base,
    independente da data de internação.
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

    cols_cid = [c for c in df.columns if c in COLS_CID]

    mask_cid = reduce(
        lambda a,b: a|b,
        [df[c].str.match(r'V[0-8][0-9]', na=False) for c in cols_cid]
    )

    mask_car_int = df[COL_CAR_INT[0]] == '05'

    mask = mask_cid | mask_car_int
    df = df.loc[mask]

    # `de_arquivo` tem o formato padronizado
    # ETLSIH.ST_UF_ANO_MES_t.csv
    # Portanto, a manipulação abaixo separa apenas o ano e mês.
    arquivo_split = de_arquivo.split('_')
    df['ano_mes_registro'] = f'{arquivo_split[-3]}-{arquivo_split[-2]}'

    return df

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
