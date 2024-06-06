import { join } from 'node:path'
import { table } from 'table'

import dfd from 'danfojs-node'
import { OUTPUT_ROOT } from '../constants.mjs'
import { writeFile } from 'node:fs/promises'
import { readFileSync } from 'node:fs'
import papa from 'papaparse'

import { stringify } from 'csv-stringify/sync'

const WRITERS = {
  csv: [
    'csv',
    (df) => {
      //
      // dfd.toCSV(df) does not escape characters, resulting
      // in broken lines in case any value contains a comma (,)
      //
      return stringify(dfd.toJSON(df), {
        header: true,
        columns: df.columns,
      })
    },
  ],
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

async function writeDfs({ outputDir, prefix = '', fmt = 'csv' }, dfsByName) {
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
      // //
      // // Contagem empreendimentos por situação da obra (geral)
      // //
      // by_situacao_obra_mdr_count: df
      //   .loc({
      //     columns: ['situacao_obra_mdr', 'cod_mdr'],
      //   })
      //   .groupby(['situacao_obra_mdr'])
      //   .count()
      //   .sortValues('cod_mdr_count', {
      //     ascending: false,
      //   }),

      // //
      // // Contagem de empreendimentos por acao (geral)
      // //
      // by_acao_count: df
      //   .loc({
      //     columns: ['acao', 'cod_mdr'],
      //   })
      //   .groupby(['acao'])
      //   .count()
      //   .sortValues('cod_mdr_count', {
      //     ascending: false,
      //   }),

      // //
      // // Contagem de empreendimentos por ano_fim_obra (geral)
      // //
      // by_ano_fim_obra_count: df
      //   .loc({
      //     columns: ['ano_fim_obra', 'cod_mdr'],
      //   })
      //   .groupby(['ano_fim_obra'])
      //   .count()
      //   .sortValues('ano_fim_obra'),

      // //
      // // Contagem de empreendimentos por ano (geral)
      // //
      // by_ano_count: df
      //   .loc({
      //     columns: ['ano', 'cod_mdr'],
      //   })
      //   .groupby(['ano'])
      //   .count()
      //   .sortValues('ano'),

      // //
      // // Contagem de empreendimentos por ano_assinatura (geral)
      // //
      // by_ano_assinatura_count: df
      //   .loc({
      //     columns: ['ano_assinatura', 'cod_mdr'],
      //   })
      //   .groupby(['ano_assinatura'])
      //   .count()
      //   .sortValues('ano_assinatura'),

      // //
      // // Contagem de empreendimentos por ano_inicio_obra (geral)
      // //
      // by_ano_inicio_obra_count: df
      //   .loc({
      //     columns: ['ano_inicio_obra', 'cod_mdr'],
      //   })
      //   .groupby(['ano_inicio_obra'])
      //   .count()
      //   .sortValues('ano_inicio_obra'),

      // //
      // // Contagem de empreendimentos por municipio
      // //
      // by_municipio_count: df
      //   .loc({
      //     columns: ['_normalized_mun_slug', 'cod_mdr'],
      //   })
      //   .groupby(['_normalized_mun_slug'])
      //   .count()
      //   .sortValues('cod_mdr_count', {
      //     ascending: false,
      //   }),

      // //
      // // By municipio
      // //
      // by_municipio_by_quadrienio_sums: df
      //   .groupby([
      //     '_normalized_mun_cod_ibge',
      //     '_normalized_mun_slug',
      //     '_quadrienio',
      //   ])
      //   .col([
      //     'vlr_investimento',
      //     'vlr_repasse_financiamento',
      //     'vlr_contrapartida',
      //   ])
      //   .sum()
      //   .sortValues('_quadrienio', {
      //     ascending: false,
      //   }),

      //
      // By regional saude
      //
      by_regional_saude_by_quadrienio_sums: df
        .groupby(['_regional_saude_id', '_regional_saude_nome', '_quadrienio'])
        .col([
          'vlr_investimento',
          'vlr_repasse_financiamento',
          'vlr_contrapartida',
        ])
        .sum()
        .sortValues('_quadrienio', {
          ascending: false,
        }),

      by_regional_saude_by_quadrienio_transporte_publico_sums: df
        .query(df['_categoria_acao'].eq('transporte_publico'))
        .groupby(['_regional_saude_id', '_regional_saude_nome', '_quadrienio'])
        .col([
          'vlr_investimento',
          'vlr_repasse_financiamento',
          'vlr_contrapartida',
        ])
        .sum()
        .sortValues('_quadrienio', {
          ascending: false,
        }),

      by_regional_saude_by_quadrienio_seguranca_viaria_sums: df
        .query(df['_categoria_acao'].eq('seguranca_viaria'))
        .groupby(['_regional_saude_id', '_regional_saude_nome', '_quadrienio'])
        .col([
          'vlr_investimento',
          'vlr_repasse_financiamento',
          'vlr_contrapartida',
        ])
        .sum()
        .sortValues('_quadrienio', {
          ascending: false,
        }),
    },
  )
}

async function run({ outputDir }) {
  const { data } = papa.parse(
    readFileSync(
      join(OUTPUT_ROOT, 'label/simu-carteira-mun-T--prepared.csv'),
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
