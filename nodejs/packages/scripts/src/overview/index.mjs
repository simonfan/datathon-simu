import { join } from 'node:path'
import { table } from 'table'

import dfd from 'danfojs-node'
import { OUTPUT_ROOT } from '../constants.mjs'
import { writeFile } from 'node:fs/promises'
import { parse } from 'csv-parse/sync'
import { readFileSync } from 'node:fs'
import papa from 'papaparse'

const CAPITAIS = {
  355030: 'São Paulo',
  330455: 'Rio de Janeiro',
  310620: 'Belo Horizonte',
  410690: 'Curitiba',
  261160: 'Recife',
  230440: 'Fortaleza',
  500270: 'Campo Grande',
  2800308: 'Aracaju',
  1501402: 'Belém',
  1400100: 'Boa Vista',
  5300108: 'Brasília',
  5103403: 'Cuiabá',
  4205407: 'Florianópolis',
  5208707: 'Goiânia',
  2507507: 'João Pessoa',
  // 1600303: 'Macapá',
  // 2704302: 'Maceió',
  1302603: 'Manaus',
  2408102: 'Natal',
  1721000: 'Palmas',
  4314902: 'Porto Alegre',
  1100205: 'Porto Velho',
  1200401: 'Rio Branco',
  2927408: 'Salvador',
  2111300: 'São Luís',
  2211001: 'Teresina',
  // 3205309: 'Vitória',
}

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
    (prev, [name, dfOrDfFn]) =>
      prev.then(async () => {
        writeFile(
          join(outputDir, `${prefix}${name}.${writer[0]}`),
          await writer[1](
            typeof dfOrDfFn === 'function' ? await dfOrDfFn() : dfOrDfFn,
          ),
        )
      }),
    Promise.resolve(),
  )
}

async function runAnalysis({ outputDir, df }) {
  await writeDfs(
    {
      outputDir,
    },
    {
      //
      // Contagem empreendimentos por situação da obra (geral)
      //
      by_situacao_obra_mdr_count: df
        .loc({
          columns: ['situacao_obra_mdr', 'cod_mdr'],
        })
        .groupby(['situacao_obra_mdr'])
        .count()
        .sortValues('cod_mdr_count', {
          ascending: false,
        }),

      //
      // Contagem de empreendimentos por acao (geral)
      //
      by_acao_count: df
        .loc({
          columns: ['acao', 'cod_mdr'],
        })
        .groupby(['acao'])
        .count()
        .sortValues('cod_mdr_count', {
          ascending: false,
        }),

      //
      // Contagem de empreendimentos por ano_fim_obra (geral)
      //
      by_ano_fim_obra_count: df
        .loc({
          columns: ['ano_fim_obra', 'cod_mdr'],
        })
        .groupby(['ano_fim_obra'])
        .count()
        .sortValues('ano_fim_obra'),

      //
      // Contagem de empreendimentos por ano (geral)
      //
      by_ano_count: df
        .loc({
          columns: ['ano', 'cod_mdr'],
        })
        .groupby(['ano'])
        .count()
        .sortValues('ano'),

      //
      // Contagem de empreendimentos por ano_inicio_obra (geral)
      //
      by_ano_inicio_obra_count: df
        .loc({
          columns: ['ano_inicio_obra', 'cod_mdr'],
        })
        .groupby(['ano_inicio_obra'])
        .count()
        .sortValues('ano_inicio_obra'),

      //
      // Contagem de empreendimentos por municipio
      //
      by_municipio_count: df
        .loc({
          columns: ['_normalized_mun_slug', 'cod_mdr'],
        })
        .groupby(['_normalized_mun_slug'])
        .count()
        .sortValues('cod_mdr_count', {
          ascending: false,
        }),

      //
      // Soma valores por município
      //
      by_municipio_sums: () => {
        const sums = df
          .groupby(['_normalized_mun_slug', '_normalized_mun_cod_ibge'])
          .col(['vlr_investimento', 'vlr_desembolsado'])
          .sum()

        const populacao = df
          .groupby(['_normalized_mun_slug', '_normalized_mun_cod_ibge'])
          .col(['Populacao'])
          .max()
          .column('Populacao_max')

        sums.addColumn(
          'vlr_investimento_por_populacao',
          sums.column('vlr_investimento_sum').div(populacao),
          { inplace: true },
        )
        sums.addColumn(
          'vlr_desembolsado_por_populacao',
          sums.column('vlr_desembolsado_sum').div(populacao),
          { inplace: true },
        )
        sums.addColumn('populacao', populacao, {
          inplace: true,
        })

        return sums.sortValues('vlr_investimento_por_populacao', {
          ascending: false,
        })
      },

      //
      // Médias valores por município
      //
      // by_municipio_means: df
      //   .groupby(['_normalized_mun_slug'])
      //   .col(['vlr_investimento'])
      //   .mean()
      //   .sortValues('vlr_investimento_mean', {
      //     ascending: false,
      //   }),
    },
  )
}

async function run({ outputDir }) {
  const { data } = papa.parse(
    readFileSync(
      join(OUTPUT_ROOT, 'prepare/simu-carteira-mun-T--prepared.csv'),
      'utf8',
    ),
    {
      header: true,
      dynamicTyping: true,
    },
  )

  const df = new dfd.DataFrame(data)

  await runAnalysis({ outputDir, df })
}

export default function ({ outputDir }) {
  return run({
    outputDir,
  })
}
