import slugify from '@sindresorhus/slugify'
import levenshtein from 'damerau-levenshtein'

import { parse } from 'csv-parse/sync'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { INPUT_ROOT } from '../constants.mjs'

export default function test() {
  const DATA = parse(
    readFileSync(join(INPUT_ROOT, 'simu-carteira-mun-T.csv'), 'utf8'),
    { columns: true },
  )

  console.log(DATA[0])
}
