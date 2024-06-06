import slugify from '@sindresorhus/slugify'

function normalizeStr(str) {
  return slugify(str)
}

function strMatcher(key, patterns) {
  patterns = (Array.isArray(patterns) ? patterns : [patterns]).map(
    (pattern) => {
      if (typeof pattern === 'string') {
        return slugify(pattern)
      } else if (pattern instanceof RegExp) {
        return pattern
      } else {
        throw new Error('Invalid pattern ' + pattern)
      }
    },
  )

  return function (entry) {
    const sourceValue = entry[key]
    const normalizedValue = normalizeStr(sourceValue)

    return patterns.some((pattern) => {
      if (typeof pattern === 'string') {
        return normalizedValue.includes(pattern)
      } else if (pattern instanceof RegExp) {
        return pattern.test(normalizedValue) || pattern.test(sourceValue)
      } else {
        throw new Error('Invalid')
      }
    })
  }
}

const EMPREENDIMENTO_CATEGORIAS = [
  ['pavimentacao', strMatcher('empreendimento', ['pavim', 'recap'])],
  ['drenagem', strMatcher('empreendimento', ['drenag'])],
  ['sinalizacao', strMatcher('empreendimento', ['sinalizacao'])],
  ['habitacional', strMatcher('empreendimento', 'habita')],
  ['transporte', strMatcher('empreendimento', 'transport')],
  ['adequacao', strMatcher('empreendimento', 'adeq')],
  ['qualificacao', strMatcher('empreendimento', 'qualific')],
]

const ACAO_CATEGORIAS = [
  [
    'transporte_publico',
    (entry) => ['10SS', '10ST', '7L51'].includes(entry.acao),
  ],
  ['seguranca_viaria', (entry) => ['8487'].includes(entry.acao)],
]

export const CATEGORY_COLUMNS = ['_categoria_empreendimento', '_categoria_acao']

export function categorize(entry) {
  const empreendimento_categoria = EMPREENDIMENTO_CATEGORIAS.find(([, test]) =>
    test(entry),
  )

  const categoria_acao = ACAO_CATEGORIAS.find(([, test]) => test(entry))

  return {
    ...entry,
    _categoria_empreendimento: empreendimento_categoria
      ? empreendimento_categoria[0]
      : '',
    _categoria_acao: categoria_acao ? categoria_acao[0] : '',
  }
}
