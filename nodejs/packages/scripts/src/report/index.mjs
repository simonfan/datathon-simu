import streamToPromise from 'stream-to-promise'
import { join } from 'node:path'

import { reportByRegionalSaude } from './reportByRegionalSaude.mjs'
import { OUTPUT_ROOT } from '../constants.mjs'

export default async function ({ outputDir }) {
  await Promise.all([
    //
    // Overall
    //
    streamToPromise(
      reportByRegionalSaude({
        inputPath: join(
          OUTPUT_ROOT,
          'aggregate/by_regional_saude_by_quadrienio_sums.csv',
        ),
        outputPath: join(outputDir, 'by_regional_saude_by_quadrienio.csv'),
        // recordCount: 90,
      }),
    ),

    //
    // Transporte publico
    //
    streamToPromise(
      reportByRegionalSaude({
        inputPath: join(
          OUTPUT_ROOT,
          'aggregate/by_regional_saude_by_quadrienio_transporte_publico_sums.csv',
        ),
        outputPath: join(
          outputDir,
          'by_regional_saude_by_quadrienio_transporte_publico.csv',
        ),
        // recordCount: 90,
      }),
    ),

    //
    // Segurança viária
    //
    streamToPromise(
      reportByRegionalSaude({
        inputPath: join(
          OUTPUT_ROOT,
          'aggregate/by_regional_saude_by_quadrienio_seguranca_viaria_sums.csv',
        ),
        outputPath: join(
          outputDir,
          'by_regional_saude_by_quadrienio_seguranca_viaria.csv',
        ),
        // recordCount: 90,
      }),
    ),
  ])
}
