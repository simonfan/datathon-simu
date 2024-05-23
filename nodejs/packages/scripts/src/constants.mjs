import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

export const INPUT_ROOT = fileURLToPath(new URL('../input', import.meta.url))
export const OUTPUT_ROOT = fileURLToPath(new URL('../output', import.meta.url))

export const SIMU_CARTEIRA_MUN_COLUMNS = readFileSync(
  join(INPUT_ROOT, 'simu-carteira-mun-T--COLUMNS.txt'),
  'utf8',
).split('\n').filter(Boolean)
