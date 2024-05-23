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

async function writeDfs({ outputDir, prefix = '' }, dfsByName, fmt = 'table') {
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

async function run({ outputDir }) {
  const df = await dfd.readCSV(
    join(OUTPUT_ROOT, 'prepare/simu-carteira-mun-T--prepared.csv'),
  )

  const counts = countDfs(df, ['situacao_obra_mdr'])

  await writeDfs({ outputDir }, counts)

  const by_mun = df.groupby(['mun_codigo_adotado', 'mun_MUNNOME'])

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

  const concluida = df.query(df['situacao_obra_mdr'].eq('CONCLUÍDA'))

  await writeDfs(
    { outputDir, prefix: 'concluida_count_group_by_' },
    countDfs(concluida, ['_categoria']),
  )
}

export default function ({ outputDir }) {
  return run({
    outputDir,
  })
}
