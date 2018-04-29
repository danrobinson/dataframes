import { expect } from 'chai'
import 'mocha'
import { Dataframe, mean, sum } from '../index'

describe('Dataframe', () => {
  let frame: Dataframe
  let unrollcalls: Dataframe
  let unvotes: Dataframe
  let airlines: Dataframe
  let airlinesWithoutDelta: Dataframe
  let flights: Dataframe
  it('should load the dataframes', () => {
    frame = Dataframe.fromCSV('./data/gapminder.csv').filter(
      row =>
        row.country !== null && row.population !== null && row.life !== null
    )
    unrollcalls = Dataframe.fromCSV('./data/unrollcalls.csv').filter(
      row => row.rcid !== null
    )
    unvotes = Dataframe.fromCSV('./data/unvotes.csv').filter(
      row => row.rcid !== null
    )
    airlines = Dataframe.fromCSV('./data/airlines.csv').filter(
      row => row.carrier !== null
    )
    airlinesWithoutDelta = airlines.filter(row => row.carrier !== 'DL')
    flights = Dataframe.fromCSV('./data/flights.csv').filter(
      row => row.year !== null
    )
  }).timeout(10000)
  describe('columnNames', () => {
    it('should have the right properties', () => {
      expect(frame.columnNames).to.have.length(6)
      expect(frame.columnNames[0]).to.equal('country')
    })
  })
  describe('columns', () => {
    it('should have the right properties', () => {
      expect(frame.columns()).to.have.length(6)
      expect(frame.columns()[0].name).to.equal('country')
    })
  })
  describe('rows', () => {
    it('should have the right properties', () => {
      expect(frame.rows()).to.have.length(15467)
    })
  })
  describe('select', () => {
    it('should select a single column', () => {
      const selected = frame.select('country')
      expect(selected.columns().length).to.equal(1)
      expect(selected.columnNames.length).to.equal(1)
      expect(selected.columns()[0].name).to.equal('country')
    })
    it('should select multiple columns', () => {
      const selected = frame.select(['country', 'year'])
      expect(selected.columns().length).to.equal(2)
      expect(selected.columnNames.length).to.equal(2)
      expect(selected.columns()[0].name).to.equal('country')
      expect(selected.columns()[1].name).to.equal('year')
      expect(selected.columnNames[0]).to.equal('country')
      expect(selected.columnNames[1]).to.equal('year')
    })
    it('should allow renaming', () => {
      const selectRenamed = frame.select({ newCountry: 'country' })
      expect(selectRenamed.columns().length).to.equal(1)
      expect(selectRenamed.columns()[0].name).to.equal('newCountry')
    })
  })
  describe('filter', () => {
    it('should be able to filter by year', () => {
      const filtered = frame.filter(row => row.year > 1900)
      expect(filtered.rows()).to.have.length(13388)
      expect(filtered.rows().every((row: any) => row.year > 1900)).to.equal(
        true
      )
    })
  })
  describe('sort', () => {
    it('should be able to sort by year', () => {
      const sorted = frame.sort('year')
      sorted.rows().reduce((previous: any, current: any) => {
        expect(previous.year <= current.year)
        return current
      })
    })
  })
  describe('mutate', () => {
    it('should add a new column', () => {
      const mutated = frame.mutate({
        gdp: row => row.income * row.population,
      })
      expect(mutated.columns()).to.have.length(7)
      expect(mutated.columnNames).to.have.length(7)
      expect(mutated.columns()[6].name).to.equal('gdp')
      expect(mutated.columnNames[6]).to.equal('gdp')
    })
  })
  describe('summarize', () => {
    it('should be able to take the mean', () => {
      const summarized = frame
        .groupBy(['country'])
        .summarize({ average_life: mean('life') })
      expect(summarized.columns()).to.have.length(2)
      expect(summarized.columnNames).to.have.length(2)
      expect(summarized.columns()[1].name).to.equal('average_life')
    })
  })
  describe('leftJoin', () => {
    it('should be able to join flight tables', () => {
      const joined = flights.leftJoin(airlines)
      expect(joined.count()).to.equal(336776)
    }).timeout(10000)
    it('should be able to join flight table without Delta', () => {
      const joined = flights.leftJoin(airlinesWithoutDelta)
      expect(joined.count()).to.equal(336776)
    })
    it('should be able to join airlines without Delta to flight table', () => {
      const joined = airlinesWithoutDelta.leftJoin(flights)
      expect(joined.count()).to.equal(288666)
    })
  })
  describe('rightJoin', () => {
    it('should be able to join flight tables', () => {
      const innerJoined = flights.innerJoin(airlines)
      expect(innerJoined.count()).to.equal(336776)
    })
    it('should be able to join flights to airlines without Delta', () => {
      const innerJoined = flights.innerJoin(airlinesWithoutDelta)
      expect(innerJoined.count()).to.equal(288666)
    })
    it('should be able to join airlines without Delta to flights', () => {
      const innerJoined = airlinesWithoutDelta.innerJoin(flights)
      expect(innerJoined.count()).to.equal(288666)
    })
    it('should be able to rename while joining', () => {
      const airlinesRenamed = airlinesWithoutDelta.select({
        code: 'carrier',
        name: 'name',
      })
      const joined = flights.rightJoin(airlinesRenamed, { carrier: 'code' })
      expect(joined.count()).to.equal(288666)
    })
  })
  describe('innerJoin', () => {
    it('should be able to join flight tables', () => {
      const joined = flights.innerJoin(airlines)
      expect(joined.count()).to.equal(336776)
    }).timeout(3000)
    it('should be able to join flight tables without delta', () => {
      const joined = flights.innerJoin(airlinesWithoutDelta)
      expect(joined.count()).to.equal(288666)
    }).timeout(3000)
  })
  describe('fullJoin', () => {
    it('should be able to join flight to airlines', () => {
      const joined = flights.fullJoin(airlines)
      expect(joined.count()).to.equal(336776)
    }).timeout(3000)
    it('should be able to join flights to airlines without Delta', () => {
      const joined = flights.fullJoin(airlinesWithoutDelta)
      expect(joined.count()).to.equal(336776)
    }).timeout(3000)
    it('should be able to join airlines without Delta to flights', () => {
      const joined = flights.fullJoin(airlinesWithoutDelta)
      expect(joined.count()).to.equal(336776)
    }).timeout(3000)
  })
})
