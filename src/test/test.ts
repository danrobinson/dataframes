import { Dataframe, mean, sum } from '../index'

const frame = Dataframe.fromCSV('./gapminder.csv').filter(
  row => row.country !== null && row.population !== null && row.life !== null
)

console.log(frame)

// console.log(frame.select('country'))
// console.log(frame.select(['country', 'year']).rows())

// console.log(frame.rows())

// console.log(frame.filter(row => row.year > 1900))

// console.log(
//   'mutate test',
//   frame
//     .filter(row => row.population !== null)
//     .mutate({ gdp: row => row.income * row.population })
//     .rows()
// )

// console.log(frame.sort('-year').table())

// console.log(
//   'mutate',
//   frame
//     .groupBy('country')
//     .mutate({
//       average_life: mean('life'),
//       difference_from_average: row => row.life - row.average_life,
//     })
//     .table()
// )

console.log(
  'summarize',
  frame.groupBy(['region', 'country']).summarize({ average_life: mean('life') })
)
