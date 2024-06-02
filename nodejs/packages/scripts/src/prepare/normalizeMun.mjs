import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import slugify from '@sindresorhus/slugify'
import { parse } from 'csv-parse/sync'
import levenshtein from 'damerau-levenshtein'

import { INPUT_ROOT } from '../constants.mjs'

const ESTADOS = parse(
  readFileSync(join(INPUT_ROOT, 'cod-ibge-estados.csv'), 'utf8'),
  {
    columns: true,
  },
)

const MUNICIPIOS = parse(
  readFileSync(join(INPUT_ROOT, 'cod-ibge-municipios.csv'), 'utf8'),
  {
    columns: true,
  },
).map((mun) => {
  const estado = ESTADOS.find((estado) => estado['COD'] === mun['cod_uf'])

  return {
    ...mun,
    slug: slugify(`${mun['nome_municipio']}-${estado.SIGLA}`),
    estado,
  }
})

const MUNICIPIOS_BY_SLUG = MUNICIPIOS.reduce((acc, entry) => {
  return {
    ...acc,
    [entry.slug]: entry,
  }
}, {})

const OVERRIDE_MAP = {
  'lauro-muller-sc': 'lauro-mueller-sc',
  'eldorado-dos-carajas-pa': 'eldorado-do-carajas-pa',
  'gouvea-mg': 'gouveia-mg',
  'santana-do-livramento-rs': 'sant-ana-do-livramento-rs',
  'fortaleza-do-tabocao-to': 'tabocao-to',
  'sao-domingos-de-pombal-pb': 'sao-domingos-pb',
  'sao-bento-de-pombal-pb': 'sao-bento-pb',
  'couto-de-magalhaes-to': 'couto-magalhaes-to',
  'sao-thome-das-letras-mg': 'sao-tome-das-letras-mg',
  // https://g1.globo.com/rn/rio-grande-do-norte/noticia/campo-grande-ou-augusto-severo-populacao-pode-decidir-nas-eleicoes-qual-o-nome-da-cidade.ghtml
  'augusto-severo-rn': 'campo-grande-rn',
  'espirito-santo-do-oeste-rn': 'parau-rn',
  'carnaubeiras-da-penha-pe': 'carnaubeira-da-penha-pe',
  'armacao-de-buzios-rj': 'armacao-dos-buzios-rj',
  'sao-valerio-da-natividade-to': 'sao-valerio-to',
  'governador-lomanto-junior-ba': 'barro-preto-ba',
  'santa-cecilia-de-umbuzeiro-pb': 'santa-cecilia-pb',
  'jamari-ro': 'candeias-do-jamari-ro',
  'paranagua-pi': 'parnagua-pi',
  'itabirinha-de-mantena-mg': 'itabirinha-mg',
  'sao-miguel-de-touros-rn': 'sao-miguel-do-gostoso-rn',
  'campo-de-santana-pb': 'tacima-pb',
  'amapari-ap': 'pedra-branca-do-amapari-ap',
  'santarem-pb': 'joca-claudino-pb',
  'embu-sp': 'embu-das-artes-sp',

  // https://pt.wikipedia.org/wiki/Palmeiras_do_Tocantins
  'mosquito-to': 'palmeiras-do-tocantins-to',
}

let CACHE_MAP = {}

export function normalizeMun(entry) {
  if (entry['mun_MUNNOMEX'] === '') {
    return entry
  }

  const slug = slugify(`${entry['mun_MUNNOMEX']}-${entry['uf_SIGLA_UF']}`)

  let mun =
    MUNICIPIOS_BY_SLUG[slug] ||
    MUNICIPIOS_BY_SLUG[OVERRIDE_MAP[slug]] ||
    CACHE_MAP[slug]

  if (!mun) {
    // console.log(`no exact: ${slug}`)

    mun = MUNICIPIOS.find((mun) => {
      if (!mun.estado) {
        console.log(mun)
      }

      return (
        entry['uf_SIGLA_UF'] === mun.estado.SIGLA &&
        slug.length === mun.slug.length &&
        levenshtein(slug, mun.slug).steps <= 2
      )
    })

    if (!mun) {
      console.log(
        `x: ${slug} - ${entry['mun_MUNNOMEX']} (${entry['uf_SIGLA_UF']})`,
      )
    } else {
      CACHE_MAP[slug] = mun
    }
  }

  return mun
    ? {
        ...entry,
        _normalized_mun_slug: mun.slug,
        _normalized_mun_nome: mun['nome_municipio'],
        _normalized_mun_cod_ibge: mun['cod_municipio'],
      }
    : entry
}

export const NORMALIZE_COLUMNS = [
  '_normalized_mun_slug',
  '_normalized_mun_nome',
  '_normalized_mun_cod_ibge',
]
