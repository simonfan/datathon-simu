import { readFileSync } from 'node:fs'
import { parse as parseSync } from 'csv-parse/sync'
import { join } from 'node:path'
import { groupBy } from 'lodash-es'

import { INPUT_ROOT } from '../constants.mjs'

import { DTB_BY_MUN_COD_IBGE } from '../label/normalizeMun.mjs'
import { csvTransformStream } from './csvTransformStream.mjs'

const FROTA = parseSync(
  readFileSync(join(INPUT_ROOT, 'simu-frota-mun_T.csv'), 'utf8'),
  {
    columns: true,
    bom: true,
  },
).map((item) => ({
  ...item,
  _normalized_mun_cod_ibge:
    DTB_BY_MUN_COD_IBGE[item['Código IBGE']].regional_id,
}))

const FROTA_BY_MUN_COD_IBGE = groupBy(FROTA, 'Código IBGE')

function _div(a, b) {
  a = parseFloat(a)
  b = parseFloat(b)

  return !Number.isNaN(a) && !Number.isNaN(b) && b > 0 ? a / b : null
}

const COLUMN_MAP = {
  _normalized_mun_cod_ibge: true,
  _normalized_mun_slug: true,
  _quadrienio: true,
  vlr_investimento_sum: true,
  vlr_repasse_financiamento_sum: true,
  vlr_contrapartida_sum: true,

  _municipio_populacao_quadrienio: (entry) => {
    const { _quadrienio, _normalized_mun_cod_ibge } = entry

    if (!_quadrienio || !_normalized_mun_cod_ibge) {
      return null
    }

    //
    // Take last year of the quadrienio
    //
    const year = parseInt(_quadrienio.split('-')[1])

    const frotaEntry = FROTA_BY_MUN_COD_IBGE[_normalized_mun_cod_ibge].find(
      (fEntry) => parseInt(fEntry.ano) === year,
    )

    return frotaEntry ? parseInt(frotaEntry['Populacao']) : null
  },

  _municipio_frota_quadrienio: (entry) => {
    const { _quadrienio, _normalized_mun_cod_ibge } = entry

    if (!_quadrienio || !_normalized_mun_cod_ibge) {
      return null
    }

    //
    // Take last year of the quadrienio
    //
    const year = parseInt(_quadrienio.split('-')[1])

    const frotaEntry = FROTA_BY_MUN_COD_IBGE[_normalized_mun_cod_ibge].find(
      (fEntry) => parseInt(fEntry.ano) === year,
    )

    return frotaEntry ? parseInt(frotaEntry['FROTA']) : null
  },

  //
  // Por hab
  //
  _vlr_investimento_por_hab: (entry) => {
    return _div(
      entry.vlr_investimento_sum,
      entry._municipio_populacao_quadrienio,
    )
  },

  _vlr_repasse_financiamento_por_hab: (entry) => {
    return _div(
      entry.vlr_repasse_financiamento_sum,
      entry._municipio_populacao_quadrienio,
    )
  },

  _vlr_contrapartida_por_hab: (entry) => {
    return _div(
      entry.vlr_contrapartida_sum,
      entry._municipio_populacao_quadrienio,
    )
  },

  //
  // Por frota
  //
  _vlr_investimento_por_frota: (entry) => {
    return _div(entry.vlr_investimento_sum, entry._municipio_frota_quadrienio)
  },

  _vlr_repasse_financiamento_por_frota: (entry) => {
    return _div(
      entry.vlr_repasse_financiamento_sum,
      entry._municipio_frota_quadrienio,
    )
  },

  _vlr_contrapartida_por_frota: (entry) => {
    return _div(entry.vlr_contrapartida_sum, entry._municipio_frota_quadrienio)
  },
}

export function reportByMunicipio(props) {
  return csvTransformStream({
    ...props,
    columnMap: COLUMN_MAP,
  })
}
