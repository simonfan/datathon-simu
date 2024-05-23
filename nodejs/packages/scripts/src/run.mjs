import { readdir, stat } from 'node:fs/promises'
import inquirer from 'inquirer'

import { fileURLToPath } from 'node:url'
import { join } from 'node:path'
import { mkdirp } from 'mkdirp'
import { OUTPUT_ROOT } from './constants.mjs'

const SCRIPTS_ROOT = fileURLToPath(new URL('.', import.meta.url))

async function loadScripts() {
  const dirs = await readdir(SCRIPTS_ROOT)

  return dirs.reduce(
    (prev, dir) =>
      prev.then(async (acc) => {
        try {
          const scriptPath = join(SCRIPTS_ROOT, dir, 'index.mjs')
          const stats = await stat(scriptPath)

          return stats.isFile()
            ? [
                ...acc,
                {
                  name: dir,
                  value: {
                    name: dir,
                    scriptPath,
                  },
                },
              ]
            : acc
        } catch (err) {
          return acc
        }
      }),
    Promise.resolve([]),
  )
}

const responses = await inquirer
  .prompt([
    {
      name: 'script',
      type: 'list',
      choices: await loadScripts(),
    },
  ])
  .then(async ({ script }) => {
    console.log(`run script ${script.name}`)
    const mod = await import(script.scriptPath)

    const outputDir = join(OUTPUT_ROOT, script.name)

    await mkdirp(outputDir)
    await mod.default({
      outputDir,
    })
  })
