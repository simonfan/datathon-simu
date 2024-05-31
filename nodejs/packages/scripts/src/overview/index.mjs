import { join } from 'node:path'
import { table } from 'table'

import dfd from 'danfojs-node'
import { OUTPUT_ROOT } from '../constants.mjs'
import { writeFile } from 'node:fs/promises'

const WRITERS = {
  csv: ['csv', (df) => dfd.toCSV(df)],
  table: [
    'table.txt',
    (df) => {
      const headers = df.columns
      const data = dfd
        .toJSON(df)
        .map((row) => headers.map((header) => row[header]))

      return table([headers, ...data])
    },
  ],
}

async function writeDfs({ outputDir, prefix = '', fmt = 'table' }, dfsByName) {
  const writer = WRITERS[fmt]

  return Object.entries(dfsByName).reduce(
    (prev, [name, df]) =>
      prev.then(async () => {
        writeFile(
          join(outputDir, `${prefix}${name}.${writer[0]}`),
          await writer[1](df),
        )
      }),
    Promise.resolve(),
  )
}

function countDfs(df, columns) {
  return columns.reduce(
    (acc, groupByCol) => ({
      ...acc,
      [groupByCol]: df
        .loc({
          columns: [groupByCol, 'cod_mdr'],
        })
        .groupby([groupByCol])
        .count()
        .sortValues('cod_mdr_count', {
          ascending: false,
        }),
    }),
    {},
  )
}

function _queryCidade(df, mun_MUNNOMEX) {
  return df.query(
    df['mun_MUNNOMEX']
      .eq(mun_MUNNOMEX)
      .and(df['situacao_obra_mdr'].eq('CONCLUÍDA')),
  )
}

async function run({ outputDir }) {
  const df = await dfd.readCSV(
    join(OUTPUT_ROOT, 'prepare/simu-carteira-mun-T--prepared.csv'),
  )

  //
  // contagens gerais
  //
  const counts = countDfs(df, ['situacao_obra_mdr', 'acao'])

  await writeDfs({ outputDir, prefix: 'count_by_' }, counts)

  //
  // Obras por município
  //
  const by_mun = df.groupby(['mun_MUNNOMEX'])

  await writeDfs(
    { outputDir },
    {
      by_mun_count: by_mun
        .col(['cod_mdr'])
        .count()
        .sortValues('cod_mdr_count', {
          ascending: false,
        }),
      by_mun_sums: by_mun
        .col(['vlr_desembolsado'])
        .sum()
        .sortValues('vlr_desembolsado_sum', {
          ascending: false,
        }),
      by_mun_means: by_mun
        .col(['vlr_desembolsado'])
        .mean()
        .sortValues('vlr_desembolsado_mean', {
          ascending: false,
        }),
    },
  )

  //
  // Obras concluidas
  //
  const concluida_cidades = dfd.concat({
    dfList: [
      _queryCidade(df, 'SÃO PAULO'),
      _queryCidade(df, 'RIO DE JANEIRO'),
      _queryCidade(df, 'BELO HORIZONTE'),
      _queryCidade(df, 'CURITIBA'),
      _queryCidade(df, 'RECIFE'),
      _queryCidade(df, 'FORTALEZA'),
    ],
    axis: 0,
  })

  //
  // Concluida by municipio by ano
  //
  await writeDfs(
    {
      outputDir,
      fmt: 'csv'
    },
    {
      concluida_by_mun: concluida_cidades
        .groupby(['mun_MUNNOMEX'])
        .col(['vlr_investimento'])
        .sum()
        .sortValues('vlr_investimento_sum', {
          ascending: false,
        }),
      concluida_by_mun_by_ano: concluida_cidades
        .groupby(['mun_MUNNOMEX', 'ano_fim_obra'])
        .col(['vlr_investimento'])
        .sum()
        .sortValues('ano_fim_obra', {
          ascending: false,
        }),
    },
  )
}

export default function ({ outputDir }) {
  return run({
    outputDir,
  })
}
