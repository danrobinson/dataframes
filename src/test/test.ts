import { Dataframe, mean, sum } from '../index'

const presidentRows = [
  {
    first_name: 'George',
    last_name: 'Washington',
    party: 'No Party',
    pres_number: 1,
    death_year: 1799,
    inauguration_year: 1789,
    last_year: 1797,
  },
  {
    first_name: 'John',
    last_name: 'Adams',
    party: 'Federalist',
    pres_number: 2,
    death_year: 1826,
    inauguration_year: 1797,
    last_year: 1801,
  },
  {
    first_name: 'Thomas',
    last_name: 'Jefferson',
    party: 'Democratic-Republican',
    pres_number: 3,
    death_year: 1826,
    inauguration_year: 1801,
    last_year: 1809,
  },
  {
    first_name: 'James',
    last_name: 'Madison',
    party: 'Democratic-Republican',
    pres_number: 4,
    death_year: 1836,
    inauguration_year: 1809,
    last_year: 1817,
  },
]

const frame = new Dataframe(undefined, presidentRows)

console.log(frame)

frame.select('party')
frame.select(['first_name', 'last_name'])

console.log(frame.rows())

// frame.select({ firstName: "first_name" })

console.log(frame.filter(row => row.death_year > 1810))

console.log(
  'mutate',
  frame
    .mutate({ full_name: row => `${row.first_name} ${row.last_name}` })
    .columns()
)

console.log('sort', frame.sort('first_name').columns())

console.log(
  'mutate',
  frame
    .groupBy('party')
    .mutate({
      average_inauguration_year: mean('inauguration_year'),
      difference_from_average: row =>
        row.inauguration_year - row.average_inauguration_year,
    })
    .rows()
)

console.log(
  'summarize',
  frame
    .groupBy('party')
    .summarize({ total_death_year: sum('inauguration_year') })
)
