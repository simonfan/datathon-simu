import { createReadStream, createWriteStream, readFileSync } from 'node:fs'
import { parse } from 'csv-parse'
import { parse as parseSync } from 'csv-parse/sync'
import { stringify } from 'csv-stringify'
import { Transform } from 'node:stream'
import { join } from 'node:path'
import { groupBy } from 'lodash-es'

import { INPUT_ROOT, OUTPUT_ROOT } from '../constants.mjs'

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
  _regional_saude_id: DTB_BY_MUN_COD_IBGE[item['Código IBGE']].regional_id,
}))

const FROTA_BY_REGIONAL_SAUDE = groupBy(FROTA, '_regional_saude_id')

function _div(a, b) {
  a = parseFloat(a)
  b = parseFloat(b)

  return !Number.isNaN(a) && !Number.isNaN(b) && b > 0 ? a / b : null
}

const COLUMN_MAP = {
  _regional_saude_id: true,
  _regional_saude_nome: true,
  _quadrienio: true,
  vlr_investimento_sum: true,
  vlr_repasse_financiamento_sum: true,
  vlr_contrapartida_sum: true,

  _regional_saude_populacao_quadrienio: (entry) => {
    const { _quadrienio, _regional_saude_id } = entry

    if (!_quadrienio || !_regional_saude_id) {
      return null
    }

    //
    // Take last year of the quadrienio
    //
    const year = parseInt(_quadrienio.split('-')[1])

    return FROTA_BY_REGIONAL_SAUDE[_regional_saude_id].reduce((acc, fEntry) => {
      return parseInt(fEntry.ano) === year
        ? acc + parseInt(fEntry['Populacao'])
        : acc
    }, 0)
  },

  _regional_saude_frota_quadrienio: (entry) => {
    const { _quadrienio, _regional_saude_id } = entry

    if (!_quadrienio || !_regional_saude_id) {
      return null
    }

    //
    // Take last year of the quadrienio
    //
    const year = parseInt(_quadrienio.split('-')[1])

    return FROTA_BY_REGIONAL_SAUDE[_regional_saude_id].reduce((acc, fEntry) => {
      return parseInt(fEntry.ano) === year &&
        !Number.isNaN(parseInt(fEntry['FROTA']))
        ? acc + parseInt(fEntry['FROTA'])
        : acc
    }, 0)
  },

  //
  // Por hab
  //
  _regional_saude_vlr_investimento_por_hab: (entry) => {
    return _div(
      entry.vlr_investimento_sum,
      entry._regional_saude_populacao_quadrienio,
    )
  },

  _regional_saude_vlr_repasse_financiamento_por_hab: (entry) => {
    return _div(
      entry.vlr_repasse_financiamento_sum,
      entry._regional_saude_populacao_quadrienio,
    )
  },

  _regional_saude_vlr_contrapartida_por_hab: (entry) => {
    return _div(
      entry.vlr_contrapartida_sum,
      entry._regional_saude_populacao_quadrienio,
    )
  },

  //
  // Por frota
  //
  _regional_saude_vlr_investimento_por_frota: (entry) => {
    return _div(
      entry.vlr_investimento_sum,
      entry._regional_saude_frota_quadrienio,
    )
  },

  _regional_saude_vlr_repasse_financiamento_por_frota: (entry) => {
    return _div(
      entry.vlr_repasse_financiamento_sum,
      entry._regional_saude_frota_quadrienio,
    )
  },

  _regional_saude_vlr_contrapartida_por_frota: (entry) => {
    return _div(
      entry.vlr_contrapartida_sum,
      entry._regional_saude_frota_quadrienio,
    )
  },
}

export function reportByRegionalSaude(props) {
  return csvTransformStream({
    ...props,
    columnMap: COLUMN_MAP,
  })
}
