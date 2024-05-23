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

const FLAGS = [
  ['pavimentacao', strMatcher('empreendimento', ['pavim', 'recap'])],
  ['drenagem', strMatcher('empreendimento', ['drenag'])],
  ['sinalizacao', strMatcher('empreendimento', ['sinalizacao'])],
  ['habitacional', strMatcher('empreendimento', 'habita')],
  ['transporte', strMatcher('empreendimento', 'transport')],
  ['adequacao', strMatcher('empreendimento', 'adeq')],
  ['qualificacao', strMatcher('empreendimento', 'qualific')],
]

export const FLAG_COLUMNS = [
  '_categoria',
  ...FLAGS.map(([label]) => `_flag_${label}`),
]

export function applyFlags(entry) {
  const flags = FLAGS.filter(([, test]) => test(entry)).map(([label]) => label)

  return {
    ...entry,
    _categoria: flags[0],
    ...flags.reduce(
      (acc, flag) => ({
        ...acc,
        [`_flag_${flag}`]: 'TRUE',
      }),
      {},
    ),
  }
}
