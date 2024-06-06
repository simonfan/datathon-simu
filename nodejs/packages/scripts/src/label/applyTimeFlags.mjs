const QUADRIENIOS = [
  [(year) => year >= 2014 && year <= 2017, '2014-2017'],
  [(year) => year >= 2018 && year <= 2021, '2018-2021'],
]

const BIENIOS = [
  [(year) => year >= 2014 && year <= 2015, '2014-2015'],
  [(year) => year >= 2016 && year <= 2017, '2016-2017'],
  [(year) => year >= 2018 && year <= 2019, '2018-2019'],
  [(year) => year >= 2020 && year <= 2021, '2020-2021'],
]

export function applyTimeFlags(entry) {
  //
  // Adiciona coluna "_quadrienio"
  // - 2014-17
  // - 2018-21
  //
  const _quadrienio =
    entry.ano && parseInt(entry.ano)
      ? QUADRIENIOS.find(([test]) => test(parseInt(entry.ano)))
      : null

  const _bienio =
    entry.ano && parseInt(entry.ano)
      ? BIENIOS.find(([test]) => test(parseInt(entry.ano)))
      : null

  return {
    ...entry,
    _quadrienio: _quadrienio ? _quadrienio[1] : '',
    _bienio: _bienio ? _bienio[1] : '',
  }
}

export const TIME_FLAG_COLUMNS = ['_quadrienio', '_bienio']
