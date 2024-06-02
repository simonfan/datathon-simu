import { createReadStream, createWriteStream } from 'node:fs'
import { parse } from 'csv-parse'
import { stringify } from 'csv-stringify'
import streamToPromise from 'stream-to-promise'
import { Transform } from 'node:stream'
import { join } from 'node:path'
import { FLAG_COLUMNS, applyFlags } from './applyFlags.mjs'

import { INPUT_ROOT, SIMU_CARTEIRA_MUN_COLUMNS } from '../constants.mjs'
import { NORMALIZE_COLUMNS, normalizeMun } from './normalizeMun.mjs'

export function prepare({ inputPath, outputPath, recordCount }) {
  const readStream = createReadStream(inputPath)
  const parser = parse({
    delimiter: ',',
    columns: true,
    to: recordCount,
  })

  const transformer = new Transform({
    transform: async function (entry, enc, callback) {
      const final = {
        ...entry,
        ...applyFlags(entry),
        ...normalizeMun(entry),
      }

      this.push(final)
      callback()
    },
    objectMode: true,
  })

  const stringifier = stringify({
    header: true,
    columns: [
      ...SIMU_CARTEIRA_MUN_COLUMNS,

      ...FLAG_COLUMNS,
      ...NORMALIZE_COLUMNS,
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
