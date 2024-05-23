import { createReadStream, createWriteStream } from 'node:fs'
import { parse } from 'csv-parse'
import { stringify } from 'csv-stringify'
import streamToPromise from 'stream-to-promise'
import { Transform } from 'node:stream'
import { join } from 'node:path'
import { FLAG_COLUMNS, applyFlags } from './applyFlags.mjs'

import { INPUT_ROOT } from '../constants.mjs'

export function prepare({ inputPath, outputPath, recordCount }) {
  const readStream = createReadStream(inputPath)
  const parser = parse({
    delimiter: ',',
    columns: true,
    to: recordCount,
  })

  const transformer = new Transform({
    transform: async function (entry, enc, callback) {
      console.log(entry['cod_mdr'])
      const withFlags = applyFlags(entry)

      this.push(withFlags)
      callback()
    },
    objectMode: true,
  })

  const stringifier = stringify({
    header: true,
    columns: [
      // ID
      'cod_mdr',
      'cod_operacao',
      'cod_saci',
      'acao',
      'programa',
      'empreendimento',
      ...FLAG_COLUMNS,
      'vlr_desembolsado',
      'situacao_obra_mdr',
      'pop_beneficiada',
      'emp_gerado',
      'vlr_investimento',
      'Código IBGE',
      'mun_codigo_adotado',
      'mun_MUNNOME',
    ],
  })

  const writeStream = createWriteStream(outputPath)

  return readStream
    .pipe(parser)
    .pipe(transformer)
    .pipe(stringifier)
    .pipe(writeStream)
}

export default async function ({ outputDir }) {
  await streamToPromise(
    prepare({
      inputPath: join(INPUT_ROOT, 'simu-carteira-mun-T.csv'),
      outputPath: join(outputDir, 'simu-carteira-mun-T--prepared.csv'),
      recordCount: undefined,
    }),
  )
}
