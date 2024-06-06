Colunas:

- _regional_saude_id:
  - tipo: string - ID da Regional de Saúde
  - fonte: DTB.csv (https://github.com/lansaviniec/shapefile_das_regionais_de_saude_sus/blob/master/DTB.csv)

- _regional_saude_nome:
  - tipo: string - Nome da Regional de Saúde
  - fonte: DTB.csv (https://github.com/lansaviniec/shapefile_das_regionais_de_saude_sus/blob/master/DTB.csv)

- _quadrienio:
  - tipo: string - Quadriênio (2014-2017 ou 2018-2021)
  - fonte: Processamento da coluna 'ano' da base de empreendimentos do SIMU (scripts/input/simu-carteira-mun-T.csv)

- vlr_investimento_sum:
  - tipo: float - Valor total investido (repasse do governo federal + contrapartida do município / estado)
  - fonte: base de empreendimentos do SIMU (scripts/input/simu-carteira-mun-T.csv)

- vlr_repasse_financiamento_sum:
  - tipo: float - Valor do repasse do governo federal
  - fonte: base de empreendimentos do SIMU (scripts/input/simu-carteira-mun-T.csv)

- vlr_contrapartida_sum:
  - tipo: float - Valor da contrapartida do município / estado
  - fonte: base de empreendimentos do SIMU (scripts/input/simu-carteira-mun-T.csv)

- _regional_saude_populacao_quadrienio:
  - tipo: integer - Soma da população de todos os municípios que integram a regional de saúde (dado da base: simu-frota-mun_T.csv)

- _regional_saude_frota_quadrienio:
  - tipo: integer - Soma da FROTA de todos os municípios que integram a regional de saúde (dado da base: simu-frota-mun_T.csv)
