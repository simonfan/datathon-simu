import { createReadStream, createWriteStream } from 'node:fs'
import { parse } from 'csv-parse'
import { stringify } from 'csv-stringify'
import { Transform } from 'node:stream'

export function csvTransformStream({
  inputPath,
  outputPath,
  recordCount,
  columnMap,
}) {
  const readStream = createReadStream(inputPath)

  const parser = parse({
    delimiter: ',',
    columns: true,
    to: recordCount,
  })

  const transformer = new Transform({
    transform: async function (entry, enc, callback) {
      const final = Object.entries(columnMap).reduce(
        (acc, [key, processor]) => {
          if (processor === true) {
            return acc
          } else if (typeof processor === 'function') {
            return {
              ...acc,
              //
              // pass acc to processor so that operations
              // may read values from previous operations
              //
              [key]: processor(acc),
            }
          }
        },
        entry,
      )

      this.push(final)
      callback()
    },
    objectMode: true,
  })

  const stringifier = stringify({
    header: true,
    columns: Object.keys(columnMap),
  })

  const writeStream = createWriteStream(outputPath)

  return readStream
    .pipe(parser)
    .pipe(transformer)
    .pipe(stringifier)
    .pipe(writeStream)
}
